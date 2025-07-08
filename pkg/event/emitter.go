package event

import (
	"context"
	"fmt"

	"github.com/dropboks/proto-event/pkg/epb"
	"github.com/dropboks/proto-event/pkg/uepb"
	"github.com/dropboks/proto-user/pkg/upb"
	"github.com/dropboks/sharedlib/utils"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"
)

type (
	Emitter interface {
		InsertUser(ctx context.Context, user *upb.User)
		UpdateUser(ctx context.Context, user *upb.User)
	}
	emitter struct {
		subject string
		logger  zerolog.Logger
		js      jetstream.JetStream
	}
)

func NewEmitter(js jetstream.JetStream, logger zerolog.Logger) Emitter {

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
	subject := viper.GetString("jetstream.event.subject.event_bus")
	return &emitter{js: js, subject: subject, logger: logger}
}

func (e *emitter) emit(ctx context.Context, s string, event *epb.EventMessage) {
	encoded, err := proto.Marshal(event)
	if err != nil {
		e.logger.Error().Err(err).Msg("failed to marshaling")
		return
	}
	sub := fmt.Sprintf("%s.%s", e.subject, s)
	if _, err := e.js.Publish(ctx, sub, encoded); err != nil {
		e.logger.Error().Err(err).Msg("failed to publish message")
	}
}

func (e *emitter) InsertUser(ctx context.Context, user *upb.User) {
	event := &epb.EventMessage{
		Event: &epb.EventMessage_UserEvent{
			UserEvent: &uepb.UserEvent{
				Event: &uepb.UserEvent_UserCreated{
					UserCreated: &uepb.UserCreated{
						Id:               user.GetId(),
						FullName:         user.GetFullName(),
						Image:            utils.StringPtr(user.GetImage()),
						Email:            user.GetEmail(),
						Password:         user.GetPassword(),
						Verified:         user.GetVerified(),
						TwoFactorEnabled: user.GetTwoFactorEnabled(),
					},
				},
			},
		},
	}
	sub := fmt.Sprintf("user.%s", user.GetId())
	e.emit(ctx, sub, event)
}

func (e *emitter) UpdateUser(ctx context.Context, user *upb.User) {
	event := &epb.EventMessage{
		Event: &epb.EventMessage_UserEvent{
			UserEvent: &uepb.UserEvent{
				Event: &uepb.UserEvent_UserUpdated{
					UserUpdated: &uepb.UserUpdated{
						Id:               user.GetId(),
						FullName:         user.GetFullName(),
						Image:            utils.StringPtr(user.GetImage()),
						Email:            user.GetEmail(),
						Password:         user.GetPassword(),
						Verified:         user.GetVerified(),
						TwoFactorEnabled: user.GetTwoFactorEnabled(),
					},
				},
			},
		},
	}
	sub := fmt.Sprintf("user.%s", user.GetId())
	e.emit(ctx, sub, event)
}
