package kafka

import (
	"context"

	kafka "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

// Handler processes a single Kafka message.
type Handler interface {
	Handle(ctx context.Context, msg kafka.Message) error
}

type Consumer struct {
	reader  *kafka.Reader
	handler Handler
}

type Config struct {
	Brokers []string
	GroupID string
	Topic   string
}

func NewConsumer(cfg Config, handler Handler) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.Brokers,
		GroupID:     cfg.GroupID,
		Topic:       cfg.Topic,
		MinBytes:    10e3, // 10 KB
		MaxBytes:    10e6, // 10 MB
		StartOffset: kafka.FirstOffset,
	})

	return &Consumer{reader: r, handler: handler}
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

		if err := c.handler.Handle(ctx, msg); err != nil {
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
