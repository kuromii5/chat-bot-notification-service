package domain

import (
	"time"

	"github.com/google/uuid"
)

// EventType mirrors the event types produced by chat-service.
// Notification-service only cares about events that require user notification.
type EventType string

const (
	EventNewQuestion   EventType = "new_question"
	EventHumanFollowUp EventType = "human_follow_up"
	EventAIReply       EventType = "ai_reply"
	EventRoomClaimed   EventType = "room_claimed"
)

// NotificationEvent is the domain representation of a Kafka event.
type NotificationEvent struct {
	ID          uuid.UUID
	Type        EventType
	RecipientID uuid.UUID
	RoomID      uuid.UUID
	SenderID    uuid.UUID
	Text        string
	OccurredAt  time.Time
}
