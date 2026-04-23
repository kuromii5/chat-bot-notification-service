package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

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
