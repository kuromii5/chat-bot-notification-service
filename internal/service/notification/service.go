package notification

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/kuromii5/notification-service/internal/domain"
)

// UserPrefsRepo fetches notification preferences for a given user.
type UserPrefsRepo interface {
	GetPreferences(ctx context.Context, userID uuid.UUID) (*domain.UserPreferences, error)
}

// ProfileRepo resolves a user's display name from their ID.
type ProfileRepo interface {
	GetUsername(ctx context.Context, userID uuid.UUID) (string, error)
}

// EmailSender delivers notifications via email.
type EmailSender interface {
	Send(ctx context.Context, email string, n domain.Notification) error
}

// IdempotencyRepo ensures each Kafka event is processed exactly once.
type IdempotencyRepo interface {
	IsProcessed(ctx context.Context, eventID uuid.UUID) (bool, error)
	MarkProcessed(ctx context.Context, eventID uuid.UUID) error
}

type Service struct {
	prefs      UserPrefsRepo
	profile    ProfileRepo
	email      EmailSender
	idempotent IdempotencyRepo
}

func NewService(
	prefs UserPrefsRepo,
	profile ProfileRepo,
	email EmailSender,
	idempotent IdempotencyRepo,
) *Service {
	return &Service{
		prefs:      prefs,
		profile:    profile,
		email:      email,
		idempotent: idempotent,
	}
}

// Notify processes a single notification event.
// Duplicate events (Kafka at-least-once) are silently skipped via idempotency check.
func (s *Service) Notify(ctx context.Context, event domain.NotificationEvent) error {
	already, err := s.idempotent.IsProcessed(ctx, event.ID)
	if err != nil {
		return fmt.Errorf("check idempotency: %w", err)
	}
	if already {
		return nil
	}

	prefs, err := s.prefs.GetPreferences(ctx, event.RecipientID)
	if err != nil {
		return fmt.Errorf("get preferences: %w", err)
	}
	if !prefs.EmailNotificationsEnabled {
		return nil
	}

	senderName, err := s.profile.GetUsername(ctx, event.SenderID)
	if err != nil {
		return fmt.Errorf("get sender name: %w", err)
	}

	n := buildNotification(event, senderName)
	if err := s.email.Send(ctx, prefs.Email, n); err != nil {
		return fmt.Errorf("send email: %w", err)
	}

	if err := s.idempotent.MarkProcessed(ctx, event.ID); err != nil {
		return fmt.Errorf("mark processed: %w", err)
	}

	return nil
}

func buildNotification(event domain.NotificationEvent, senderName string) domain.Notification {
	switch event.Type {
	case domain.EventNewQuestion:
		return domain.Notification{
			Subject: "New question from " + senderName,
			Body: fmt.Sprintf(
				"%s is waiting for your answer in room %s.",
				senderName,
				event.RoomID,
			),
		}
	case domain.EventHumanFollowUp:
		return domain.Notification{
			Subject: "Follow-up from " + senderName,
			Body:    fmt.Sprintf("%s sent a follow-up: %q", senderName, event.Text),
		}
	case domain.EventAIReply:
		return domain.Notification{
			Subject: "New reply from " + senderName,
			Body:    fmt.Sprintf("%s replied: %q", senderName, event.Text),
		}
	case domain.EventRoomClaimed:
		return domain.Notification{
			Subject: "Your room was claimed",
			Body:    fmt.Sprintf("%s joined your room %s.", senderName, event.RoomID),
		}
	default:
		return domain.Notification{
			Subject: "New notification",
			Body:    event.Text,
		}
	}
}
