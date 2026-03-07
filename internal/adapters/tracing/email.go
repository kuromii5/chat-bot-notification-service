package tracing

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/kuromii5/notification-service/internal/domain"
)

type emailSender interface {
	Send(ctx context.Context, to string, n domain.Notification) error
}

const emailTracer = "email"

// EmailSender wraps any emailSender and adds an OTel span to each delivery attempt.
type EmailSender struct {
	inner emailSender
}

func NewEmailSender(inner emailSender) *EmailSender {
	return &EmailSender{inner: inner}
}

func (s *EmailSender) Send(ctx context.Context, to string, n domain.Notification) error {
	ctx, span := otel.Tracer(emailTracer).Start(ctx, "email.Send")
	defer span.End()
	span.SetAttributes(
		attribute.String("email.subject", n.Subject),
	)

	err := s.inner.Send(ctx, to, n)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}
