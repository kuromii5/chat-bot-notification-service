package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/kuromii5/notification-service/internal/domain"
)

type userRow struct {
	ID                        uuid.UUID `db:"id"`
	Email                     string    `db:"email"`
	EmailNotificationsEnabled bool      `db:"email_notifications_enabled"`
}

func (db *DB) GetPreferences(ctx context.Context, userID uuid.UUID) (*domain.UserPreferences, error) {
	var row userRow
	if err := db.GetContext(ctx, &row, getPreferencesQuery, userID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.ErrPreferencesNotFound
		}
		return nil, fmt.Errorf("get preferences: %w", err)
	}

	return &domain.UserPreferences{
		UserID:                    row.ID,
		Email:                     row.Email,
		EmailNotificationsEnabled: row.EmailNotificationsEnabled,
	}, nil
}

// GetUsername fetches the display name of a user from core.profiles.
// Used by the Kafka consumer to populate SenderName in notification events.
func (db *DB) GetUsername(ctx context.Context, userID uuid.UUID) (string, error) {
	var username string
	if err := db.GetContext(ctx, &username, getUsernameQuery, userID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("get username: %w", err)
	}
	return username, nil
}
