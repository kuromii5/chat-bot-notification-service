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

// NotificationService is the port this consumer drives.
type NotificationService interface {
	Notify(ctx context.Context, event domain.NotificationEvent) error
}

// ProfileRepo resolves a user's display name from their ID.
type ProfileRepo interface {
	GetUsername(ctx context.Context, userID uuid.UUID) (string, error)
}

type Consumer struct {
	reader  *kafka.Reader
	svc     NotificationService
	profile ProfileRepo
}

type Config struct {
	Brokers []string
	GroupID string
	Topic   string
}

func NewConsumer(cfg Config, svc NotificationService, profile ProfileRepo) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.Brokers,
		GroupID:     cfg.GroupID,
		Topic:       cfg.Topic,
		MinBytes:    10e3, // 10 KB
		MaxBytes:    10e6, // 10 MB
		StartOffset: kafka.FirstOffset,
	})

	return &Consumer{reader: r, svc: svc, profile: profile}
}

func (c *Consumer) Run(ctx context.Context) {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // shutdown
			}
			logrus.WithError(err).Error("kafka: fetch message failed")
			continue
		}

		if err := c.handle(ctx, msg); err != nil {
			logrus.WithError(err).
				WithField("offset", msg.Offset).
				Error("kafka: handle message failed")
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			logrus.WithError(err).Error("kafka: commit message failed")
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

func (c *Consumer) handle(ctx context.Context, msg kafka.Message) error {
	var raw rawEvent
	if err := json.Unmarshal(msg.Value, &raw); err != nil {
		return fmt.Errorf("unmarshal event: %w", err)
	}

	senderName, err := c.profile.GetUsername(ctx, raw.SenderID)
	if err != nil {
		logrus.WithError(err).WithField("sender_id", raw.SenderID).Warn("kafka: get sender name failed, using empty")
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

	if err := c.svc.Notify(ctx, event); err != nil {
		return fmt.Errorf("notify: %w", err)
	}

	return nil
}
