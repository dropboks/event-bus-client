package event

import (
	"context"

	"github.com/dropboks/proto-event/pkg/epb"
	"github.com/dropboks/proto-event/pkg/uepb"
	"github.com/dropboks/proto-user/pkg/upb"
	"github.com/dropboks/sharedlib/utils"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"
)

type (
	UserEventConsumer interface {
		StartConsume()
	}
	userEventConsumer struct {
		pgx        *pgxpool.Pool
		jsConsumer jetstream.Consumer
		logger     zerolog.Logger
	}
)

func NewUserEventConsumer(pgx *pgxpool.Pool, js jetstream.JetStream, streamCfg jetstream.ConsumerConfig, logger zerolog.Logger) UserEventConsumer {
	cfg := &jetstream.StreamConfig{
		Name:        viper.GetString("jetstream.event.stream.name"),
		Description: viper.GetString("jetstream.event.stream.description"),
		Subjects:    []string{viper.GetString("jetstream.event.subject.global")},
		MaxBytes:    6 * 1024 * 1024,
		Storage:     jetstream.FileStorage,
	}
	_, err := js.CreateOrUpdateStream(context.Background(), *cfg)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create or update JetStream Event Bus stream")
	}
	ctx := context.Background()
	// create based on config passed
	con, err := js.CreateOrUpdateConsumer(ctx, viper.GetString("jetstream.event.stream.name"), streamCfg)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create or update consumer")
		return nil
	}

	return &userEventConsumer{
		pgx:        pgx,
		jsConsumer: con,
		logger:     logger,
	}
}

func (u *userEventConsumer) StartConsume() {
	handler := NewUserEventConsumerHandler(u.pgx, u.logger)
	_, err := u.jsConsumer.Consume(func(msg jetstream.Msg) {
		event := &epb.EventMessage{}
		if err := proto.Unmarshal(msg.Data(), event); err != nil {
			u.logger.Error().Err(err).Msg("failed to unmarshal event message")
			_ = msg.Nak()
			return
		}

		switch evt := event.Event.(type) {
		case *epb.EventMessage_UserEvent:
			switch userEvt := evt.UserEvent.Event.(type) {
			case *uepb.UserEvent_UserCreated:
				u.logger.Info().Str("user_id", userEvt.UserCreated.GetId()).Msg("UserCreated event received")
				um := userEvt.UserCreated
				us := &upb.User{
					Id:               um.GetId(),
					FullName:         um.GetFullName(),
					Image:            utils.StringPtr(um.GetImage()),
					Email:            um.GetEmail(),
					Password:         um.GetPassword(),
					Verified:         um.GetVerified(),
					TwoFactorEnabled: um.GetTwoFactorEnabled(),
				}

				err := handler.InsertUser(context.Background(), us)

				if err != nil {
					u.logger.Error().Err(err).Msg("failed to insert user")
				}
				msg.Ack()

			case *uepb.UserEvent_UserUpdated:
				u.logger.Info().Str("user_id", userEvt.UserUpdated.GetId()).Msg("UserUpdate event received")

				um := userEvt.UserUpdated
				us := &upb.User{
					Id:               um.GetId(),
					FullName:         um.GetFullName(),
					Image:            utils.StringPtr(um.GetImage()),
					Email:            um.GetEmail(),
					Password:         um.GetPassword(),
					Verified:         um.GetVerified(),
					TwoFactorEnabled: um.GetTwoFactorEnabled(),
				}

				handler.UpdateUser(context.Background(), us)

				if err := msg.Ack(); err != nil {
					u.logger.Error().Err(err).Msg("failed to ack UserUpdated event")
				}

			default:
				u.logger.Warn().Msg("unknown user event type")
				_ = msg.Nak()
			}
		default:
			u.logger.Warn().Msg("unknown event type")
			_ = msg.Nak()
		}
	})
	if err != nil {
		u.logger.Fatal().Err(err).Msg("failed to consume event bus stream")
	}
}
