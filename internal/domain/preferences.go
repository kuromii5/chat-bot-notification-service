package domain

import "github.com/google/uuid"

// UserPreferences holds the contact info needed to reach a user via email.
type UserPreferences struct {
	UserID                    uuid.UUID
	Email                     string
	EmailNotificationsEnabled bool
}

// Notification is the message delivered to the user.
type Notification struct {
	Subject string
	Body    string
}
