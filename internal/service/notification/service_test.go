package notification_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kuromii5/notification-service/internal/domain"
	"github.com/kuromii5/notification-service/internal/service/notification"
	"github.com/kuromii5/notification-service/internal/service/notification/mocks"
)

func newService(t *testing.T) (
	*notification.Service,
	*mocks.MockUserPrefsRepo,
	*mocks.MockProfileRepo,
	*mocks.MockEmailSender,
	*mocks.MockIdempotencyRepo,
) {
	t.Helper()
	prefs := mocks.NewMockUserPrefsRepo(t)
	profile := mocks.NewMockProfileRepo(t)
	email := mocks.NewMockEmailSender(t)
	idempotent := mocks.NewMockIdempotencyRepo(t)
	svc := notification.NewService(prefs, profile, email, idempotent)
	return svc, prefs, profile, email, idempotent
}

func TestNotify_AlreadyProcessed(t *testing.T) {
	svc, _, _, _, idempotent := newService(t)
	event := domain.NotificationEvent{ID: uuid.New()}
	idempotent.EXPECT().IsProcessed(context.Background(), event.ID).Return(true, nil)

	err := svc.Notify(context.Background(), event)
	require.NoError(t, err)
}

func TestNotify_IdempotencyCheckError(t *testing.T) {
	svc, _, _, _, idempotent := newService(t)
	event := domain.NotificationEvent{ID: uuid.New()}
	idempotent.EXPECT().IsProcessed(context.Background(), event.ID).Return(false, errors.New("db error"))

	err := svc.Notify(context.Background(), event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "check idempotency")
}

func TestNotify_NotificationsDisabled(t *testing.T) {
	svc, prefsRepo, _, _, idempotent := newService(t)
	event := domain.NotificationEvent{ID: uuid.New(), RecipientID: uuid.New()}
	idempotent.EXPECT().IsProcessed(context.Background(), event.ID).Return(false, nil)
	prefsRepo.EXPECT().GetPreferences(context.Background(), event.RecipientID).Return(
		&domain.UserPreferences{EmailNotificationsEnabled: false}, nil,
	)

	err := svc.Notify(context.Background(), event)
	require.NoError(t, err)
}

func TestNotify_GetPreferencesError(t *testing.T) {
	svc, prefsRepo, _, _, idempotent := newService(t)
	event := domain.NotificationEvent{ID: uuid.New(), RecipientID: uuid.New()}
	idempotent.EXPECT().IsProcessed(context.Background(), event.ID).Return(false, nil)
	prefsRepo.EXPECT().GetPreferences(context.Background(), event.RecipientID).Return(nil, errors.New("db error"))

	err := svc.Notify(context.Background(), event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get preferences")
}

func TestNotify_GetUsernameError(t *testing.T) {
	svc, prefsRepo, profileRepo, _, idempotent := newService(t)
	event := domain.NotificationEvent{ID: uuid.New(), RecipientID: uuid.New(), SenderID: uuid.New()}
	idempotent.EXPECT().IsProcessed(context.Background(), event.ID).Return(false, nil)
	prefsRepo.EXPECT().GetPreferences(context.Background(), event.RecipientID).Return(
		&domain.UserPreferences{Email: "user@example.com", EmailNotificationsEnabled: true}, nil,
	)
	profileRepo.EXPECT().GetUsername(context.Background(), event.SenderID).Return("", errors.New("not found"))

	err := svc.Notify(context.Background(), event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get sender name")
}

func TestNotify_SendEmailError(t *testing.T) {
	svc, prefsRepo, profileRepo, emailSender, idempotent := newService(t)
	event := domain.NotificationEvent{
		ID:          uuid.New(),
		RecipientID: uuid.New(),
		SenderID:    uuid.New(),
		Type:        domain.EventAIReply,
		Text:        "hello",
	}
	prefs := &domain.UserPreferences{Email: "user@example.com", EmailNotificationsEnabled: true}
	idempotent.EXPECT().IsProcessed(context.Background(), event.ID).Return(false, nil)
	prefsRepo.EXPECT().GetPreferences(context.Background(), event.RecipientID).Return(prefs, nil)
	profileRepo.EXPECT().GetUsername(context.Background(), event.SenderID).Return("Alice", nil)
	emailSender.EXPECT().Send(context.Background(), prefs.Email, domain.Notification{
		Subject: "New reply from Alice",
		Body:    `Alice replied: "hello"`,
	}).Return(errors.New("smtp error"))

	err := svc.Notify(context.Background(), event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "send email")
}

func TestNotify_MarkProcessedError(t *testing.T) {
	svc, prefsRepo, profileRepo, emailSender, idempotent := newService(t)
	event := domain.NotificationEvent{
		ID:          uuid.New(),
		RecipientID: uuid.New(),
		SenderID:    uuid.New(),
		Type:        domain.EventAIReply,
		Text:        "hello",
	}
	prefs := &domain.UserPreferences{Email: "user@example.com", EmailNotificationsEnabled: true}
	idempotent.EXPECT().IsProcessed(context.Background(), event.ID).Return(false, nil)
	prefsRepo.EXPECT().GetPreferences(context.Background(), event.RecipientID).Return(prefs, nil)
	profileRepo.EXPECT().GetUsername(context.Background(), event.SenderID).Return("Alice", nil)
	emailSender.EXPECT().Send(context.Background(), prefs.Email, domain.Notification{
		Subject: "New reply from Alice",
		Body:    `Alice replied: "hello"`,
	}).Return(nil)
	idempotent.EXPECT().MarkProcessed(context.Background(), event.ID).Return(errors.New("db error"))

	err := svc.Notify(context.Background(), event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mark processed")
}

func TestNotify_Success_EventTypes(t *testing.T) {
	roomID := uuid.New()
	tests := []struct {
		name            string
		eventType       domain.EventType
		text            string
		expectedSubject string
		expectedBody    string
	}{
		{
			name:            "new question",
			eventType:       domain.EventNewQuestion,
			expectedSubject: "New question from Bob",
			expectedBody:    "Bob is waiting for your answer in room " + roomID.String() + ".",
		},
		{
			name:            "human follow up",
			eventType:       domain.EventHumanFollowUp,
			text:            "still waiting",
			expectedSubject: "Follow-up from Bob",
			expectedBody:    `Bob sent a follow-up: "still waiting"`,
		},
		{
			name:            "ai reply",
			eventType:       domain.EventAIReply,
			text:            "here is your answer",
			expectedSubject: "New reply from Bob",
			expectedBody:    `Bob replied: "here is your answer"`,
		},
		{
			name:            "room claimed",
			eventType:       domain.EventRoomClaimed,
			expectedSubject: "Your room was claimed",
			expectedBody:    "Bob joined your room " + roomID.String() + ".",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc, prefsRepo, profileRepo, emailSender, idempotent := newService(t)
			event := domain.NotificationEvent{
				ID:          uuid.New(),
				RecipientID: uuid.New(),
				SenderID:    uuid.New(),
				Type:        tc.eventType,
				RoomID:      roomID,
				Text:        tc.text,
			}
			prefs := &domain.UserPreferences{Email: "user@example.com", EmailNotificationsEnabled: true}
			expectedNotif := domain.Notification{Subject: tc.expectedSubject, Body: tc.expectedBody}

			idempotent.EXPECT().IsProcessed(context.Background(), event.ID).Return(false, nil)
			prefsRepo.EXPECT().GetPreferences(context.Background(), event.RecipientID).Return(prefs, nil)
			profileRepo.EXPECT().GetUsername(context.Background(), event.SenderID).Return("Bob", nil)
			emailSender.EXPECT().Send(context.Background(), prefs.Email, expectedNotif).Return(nil)
			idempotent.EXPECT().MarkProcessed(context.Background(), event.ID).Return(nil)

			err := svc.Notify(context.Background(), event)
			require.NoError(t, err)
		})
	}
}
