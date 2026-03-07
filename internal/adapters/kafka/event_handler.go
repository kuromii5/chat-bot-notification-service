package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"

	"github.com/kuromii5/notification-service/internal/domain"
)

// rawEvent mirrors the NotificationEvent produced by chat-service's Kafka adapter.
type rawEvent struct {
	ID          uuid.UUID        `json:"id"`
	Type        domain.EventType `json:"type"`
	RecipientID uuid.UUID        `json:"recipient_id"`
	RoomID      uuid.UUID        `json:"room_id"`
	SenderID    uuid.UUID        `json:"sender_id"`
	Text        string           `json:"text"`
	OccurredAt  time.Time        `json:"occurred_at"`
}

// NotificationService is the port this handler drives.
type NotificationService interface {
	Notify(ctx context.Context, event domain.NotificationEvent) error
}

// ProfileRepo resolves a user's display name from their ID.
type ProfileRepo interface {
	GetUsername(ctx context.Context, userID uuid.UUID) (string, error)
}

// EventHandler unmarshals a raw Kafka message into a domain event and delivers it.
type EventHandler struct {
	svc     NotificationService
	profile ProfileRepo
}

func NewEventHandler(svc NotificationService, profile ProfileRepo) *EventHandler {
	return &EventHandler{svc: svc, profile: profile}
}

func (h *EventHandler) Handle(ctx context.Context, msg kafka.Message) error {
	var raw rawEvent
	if err := json.Unmarshal(msg.Value, &raw); err != nil {
		return fmt.Errorf("unmarshal event: %w", err)
	}

	senderName, err := h.profile.GetUsername(ctx, raw.SenderID)
	if err != nil {
		logrus.WithError(err).
			WithField("sender_id", raw.SenderID).
			Warn("kafka: get sender name failed, using empty")
	}

	event := domain.NotificationEvent{
		ID:          raw.ID,
		Type:        raw.Type,
		RecipientID: raw.RecipientID,
		RoomID:      raw.RoomID,
		SenderName:  senderName,
		Text:        raw.Text,
		OccurredAt:  raw.OccurredAt,
	}

	if err := h.svc.Notify(ctx, event); err != nil {
		return fmt.Errorf("notify: %w", err)
	}

	return nil
}
