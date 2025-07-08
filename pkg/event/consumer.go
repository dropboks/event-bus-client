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
	"google.golang.org/protobuf/proto"
)

// recieve instance pgx cause we won't accept custom function and will
// just update the database cause we don't want other behaviour since
// the utility of these event just to sync data across databases

type (
	EventConsumer interface {
		StartConsume()
	}
	eventConsumer struct {
		pgx        *pgxpool.Pool
		jsConsumer jetstream.Consumer
		logger     zerolog.Logger
	}
)

func NewEventConsumer(pgx *pgxpool.Pool, jsConsumer jetstream.Consumer, logger zerolog.Logger) EventConsumer {
	return &eventConsumer{
		pgx:        pgx,
		jsConsumer: jsConsumer,
		logger:     logger,
	}
}

// run this function in goroutin from you entrypoint app
func (e *eventConsumer) StartConsume() {
	handler := NewConsumerHandler(e.pgx, e.logger)
	_, err := e.jsConsumer.Consume(func(msg jetstream.Msg) {
		event := &epb.EventMessage{}
		if err := proto.Unmarshal(msg.Data(), event); err != nil {
			e.logger.Error().Err(err).Msg("failed to unmarshal event message")
			_ = msg.Nak()
			return
		}

		switch evt := event.Event.(type) {
		case *epb.EventMessage_UserEvent:
			switch userEvt := evt.UserEvent.Event.(type) {
			case *uepb.UserEvent_UserCreated:
				e.logger.Info().Str("user_id", userEvt.UserCreated.GetId()).Msg("UserCreated event received")
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
					e.logger.Error().Err(err).Msg("failed to insert user")
				}
				msg.Ack()

			case *uepb.UserEvent_UserUpdated:
				e.logger.Info().Str("user_id", userEvt.UserUpdated.GetId()).Msg("UserUpdate event received")

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
					e.logger.Error().Err(err).Msg("failed to ack UserUpdated event")
				}

			default:
				e.logger.Warn().Msg("unknown user event type")
				_ = msg.Nak()
			}
		default:
			e.logger.Warn().Msg("unknown event type")
			_ = msg.Nak()
		}
	})
	if err != nil {
		e.logger.Fatal().Err(err).Msg("failed to consume event bus stream")
	}
}
