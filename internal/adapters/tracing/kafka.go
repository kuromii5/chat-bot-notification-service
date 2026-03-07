package tracing

import (
	"context"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

type handler interface {
	Handle(ctx context.Context, msg kafka.Message) error
}

const kafkaTracer = "kafka/consumer"

type KafkaHandler struct {
	inner handler
}

func NewKafka(inner handler) *KafkaHandler {
	return &KafkaHandler{inner: inner}
}

// kafkaHeaderCarrier adapts []kafka.Header to propagation.TextMapCarrier
// so that OTel trace context injected by the producer can be extracted here.
type kafkaHeaderCarrier []kafka.Header

func (c kafkaHeaderCarrier) Get(key string) string {
	for _, h := range c {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c kafkaHeaderCarrier) Set(_ string, _ string) {}

func (c kafkaHeaderCarrier) Keys() []string {
	keys := make([]string, len(c))
	for i, h := range c {
		keys[i] = h.Key
	}
	return keys
}

func (k *KafkaHandler) Handle(ctx context.Context, msg kafka.Message) (err error) {
	// Extract trace context propagated by the producer (W3C TraceContext).
	ctx = otel.GetTextMapPropagator().Extract(ctx, kafkaHeaderCarrier(msg.Headers))

	ctx, span := otel.Tracer(kafkaTracer).Start(ctx, "kafka.Handle")
	defer span.End()
	span.SetAttributes(
		attribute.Int64("messaging.kafka.offset", msg.Offset),
		attribute.String("messaging.kafka.topic", msg.Topic),
	)

	if err = k.inner.Handle(ctx, msg); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return err
}
