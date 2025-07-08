package event

import (
	"context"

	sq "github.com/Masterminds/squirrel"
	"github.com/dropboks/proto-user/pkg/upb"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

type (
	UserEventHandler interface {
		InsertUser(ctx context.Context, user *upb.User) error
		UpdateUser(ctx context.Context, user *upb.User) error
	}
	userEventhandler struct {
		pgx    *pgxpool.Pool
		logger zerolog.Logger
	}
)

func NewUserEventConsumerHandler(pgx *pgxpool.Pool, logger zerolog.Logger) UserEventHandler {
	return &userEventhandler{
		pgx:    pgx,
		logger: logger,
	}
}

func (u *userEventhandler) InsertUser(ctx context.Context, user *upb.User) error {

	query, args, err := sq.Insert("users").
		Columns("id", "full_name", "image", "email", "password", "verified", "two_factor_enabled").
		Values(user.GetId(), user.GetFullName(), user.GetImage(), user.GetEmail(), user.GetPassword(), user.GetVerified(), user.GetTwoFactorEnabled()).
		Suffix("RETURNING id").
		PlaceholderFormat(sq.Dollar).
		ToSql()
	if err != nil {
		u.logger.Error().Err(err).Msg("failed to build insert query")
		return err
	}
	row := u.pgx.QueryRow(context.Background(), query, args...)
	var id string
	if err := row.Scan(&id); err != nil {
		u.logger.Error().Err(err).Msg("failed to insert user")
		return err
	}
	return nil
}

func (u *userEventhandler) UpdateUser(ctx context.Context, user *upb.User) error {
	query, args, err := sq.Update("users").
		Set("full_name", user.FullName).
		Set("image", user.Image).
		Set("email", user.Email).
		Set("password", user.Password).
		Set("verified", user.Verified).
		Set("two_factor_enabled", user.TwoFactorEnabled).
		Set("updated_at", sq.Expr("CURRENT_TIMESTAMP")).
		Where(sq.Eq{"id": user.GetId()}).
		PlaceholderFormat(sq.Dollar).
		ToSql()
	if err != nil {
		u.logger.Error().Err(err).Msg("failed to build update query")
		return err
	}

	cmdTag, err := u.pgx.Exec(ctx, query, args...)
	if err != nil {
		u.logger.Error().Err(err).Msg("failed to update user")
		return err
	}
	if cmdTag.RowsAffected() == 0 {
		u.logger.Warn().Str("id", user.GetId()).Msg("user not found for update")
		return err
	}
	return nil
}
