package tracing

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/kuromii5/notification-service/internal/domain"
)

type notifier interface {
	Notify(ctx context.Context, event domain.NotificationEvent) error
}

const svcTracer = "service/notification"

// NotificationService wraps the notification service and adds an OTel span to Notify.
type NotificationService struct {
	inner notifier
}

func NewNotificationService(inner notifier) *NotificationService {
	return &NotificationService{inner: inner}
}

func (s *NotificationService) Notify(ctx context.Context, event domain.NotificationEvent) error {
	ctx, span := otel.Tracer(svcTracer).Start(ctx, "notification.Notify")
	defer span.End()
	span.SetAttributes(
		attribute.String("event.id", event.ID.String()),
		attribute.String("event.type", string(event.Type)),
		attribute.String("event.recipient_id", event.RecipientID.String()),
		attribute.String("event.room_id", event.RoomID.String()),
	)

	err := s.inner.Notify(ctx, event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}
