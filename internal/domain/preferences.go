package domain

import "github.com/google/uuid"

// UserPreferences holds the contact info needed to reach a user via email.
type UserPreferences struct {
	UserID                    uuid.UUID `db:"id"`
	Email                     string    `db:"email"`
	EmailNotificationsEnabled bool      `db:"email_notifications_enabled"`
}

// Notification is the message delivered to the user.
type Notification struct {
	Subject string
	Body    string
}
