//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetUsername_Success(t *testing.T) {
	truncateAll(t)
	userID, username := createTestUser(t, "username@test.com", true)

	result, err := testRepo.GetUsername(context.Background(), uuid.MustParse(userID))
	require.NoError(t, err)
	assert.Equal(t, username, result)
}

func TestGetUsername_NotFound(t *testing.T) {
	truncateAll(t)

	result, err := testRepo.GetUsername(context.Background(), uuid.New())
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestIsProcessed_False(t *testing.T) {
	truncateAll(t)

	processed, err := testRepo.IsProcessed(context.Background(), uuid.New())
	require.NoError(t, err)
	assert.False(t, processed)
}

func TestMarkProcessed_Success(t *testing.T) {
	truncateAll(t)
	eventID := uuid.New()

	err := testRepo.MarkProcessed(context.Background(), eventID)
	require.NoError(t, err)

	processed, err := testRepo.IsProcessed(context.Background(), eventID)
	require.NoError(t, err)
	assert.True(t, processed)
}

func TestMarkProcessed_Idempotent(t *testing.T) {
	truncateAll(t)
	eventID := uuid.New()

	require.NoError(t, testRepo.MarkProcessed(context.Background(), eventID))
	require.NoError(t, testRepo.MarkProcessed(context.Background(), eventID))

	var count int
	err := testDB.Get(&count, `SELECT COUNT(*) FROM notification.processed_events WHERE event_id = $1`, eventID)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}
