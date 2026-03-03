package domain

import "errors"

var (
	ErrPreferencesNotFound = errors.New("user preferences not found")
	ErrAlreadyProcessed    = errors.New("event already processed")
)
