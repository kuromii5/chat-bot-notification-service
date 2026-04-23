package tracing

import (
	"context"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

)

// postgresRepo is the union of all repo interfaces consumed by the service and kafka layers.
// Repo satisfies them all via duck typing — no consumer packages are imported here.
type postgresRepo interface {
	GetUsername(ctx context.Context, userID uuid.UUID) (string, error)
	IsProcessed(ctx context.Context, eventID uuid.UUID) (bool, error)
	MarkProcessed(ctx context.Context, eventID uuid.UUID) error
}

const dbTracer = "postgres"

// Repo wraps any postgresRepo and adds an OTel span to every DB call.
type Repo struct {
	inner postgresRepo
}

func NewRepo(inner postgresRepo) *Repo {
	return &Repo{inner: inner}
}

func (r *Repo) GetUsername(ctx context.Context, userID uuid.UUID) (string, error) {
	ctx, span := otel.Tracer(dbTracer).Start(ctx, "postgres.GetUsername")
	defer span.End()
	span.SetAttributes(
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "core.profiles"),
		attribute.String("user.id", userID.String()),
	)

	result, err := r.inner.GetUsername(ctx, userID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return result, err
}

func (r *Repo) IsProcessed(ctx context.Context, eventID uuid.UUID) (bool, error) {
	ctx, span := otel.Tracer(dbTracer).Start(ctx, "postgres.IsProcessed")
	defer span.End()
	span.SetAttributes(
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "notification.processed_events"),
		attribute.String("event.id", eventID.String()),
	)

	result, err := r.inner.IsProcessed(ctx, eventID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return result, err
}

func (r *Repo) MarkProcessed(ctx context.Context, eventID uuid.UUID) error {
	ctx, span := otel.Tracer(dbTracer).Start(ctx, "postgres.MarkProcessed")
	defer span.End()
	span.SetAttributes(
		attribute.String("db.operation", "INSERT"),
		attribute.String("db.table", "notification.processed_events"),
		attribute.String("event.id", eventID.String()),
	)

	err := r.inner.MarkProcessed(ctx, eventID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}
