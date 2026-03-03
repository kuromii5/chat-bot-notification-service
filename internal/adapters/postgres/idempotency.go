package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

func (db *DB) IsProcessed(ctx context.Context, eventID uuid.UUID) (bool, error) {
	var exists bool
	if err := db.GetContext(ctx, &exists, isProcessedQuery, eventID); err != nil {
		return false, fmt.Errorf("check processed: %w", err)
	}
	return exists, nil
}

func (db *DB) MarkProcessed(ctx context.Context, eventID uuid.UUID) error {
	if _, err := db.ExecContext(ctx, markProcessedQuery, eventID); err != nil {
		return fmt.Errorf("mark processed: %w", err)
	}
	return nil
}
