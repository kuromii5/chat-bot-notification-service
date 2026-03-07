package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"

	"github.com/kuromii5/notification-service/internal/domain"
)

// NotificationService is the port this handler drives.
type NotificationService interface {
	Notify(ctx context.Context, event domain.NotificationEvent) error
}

// kafkaEvent is the wire-format DTO for events produced by chat-service.
// It lives in the adapter layer and is never exposed to the domain or service.
type kafkaEvent struct {
	ID          uuid.UUID        `json:"id"`
	Type        domain.EventType `json:"type"`
	RecipientID uuid.UUID        `json:"recipient_id"`
	RoomID      uuid.UUID        `json:"room_id"`
	SenderID    uuid.UUID        `json:"sender_id"`
	Text        string           `json:"text"`
	OccurredAt  time.Time        `json:"occurred_at"`
}

// EventHandler unmarshals a raw Kafka message into a domain event and delivers it.
// It has no repository dependencies — sender name resolution is the service's concern.
type EventHandler struct {
	svc NotificationService
}

func NewEventHandler(svc NotificationService) *EventHandler {
	return &EventHandler{svc: svc}
}

func (h *EventHandler) Handle(ctx context.Context, msg kafka.Message) error {
	var raw kafkaEvent
	if err := json.Unmarshal(msg.Value, &raw); err != nil {
		return fmt.Errorf("unmarshal event: %w", err)
	}

	event := domain.NotificationEvent{
		ID:          raw.ID,
		Type:        raw.Type,
		RecipientID: raw.RecipientID,
		RoomID:      raw.RoomID,
		SenderID:    raw.SenderID,
		Text:        raw.Text,
		OccurredAt:  raw.OccurredAt,
	}

	if err := h.svc.Notify(ctx, event); err != nil {
		return fmt.Errorf("notify: %w", err)
	}

	return nil
}
