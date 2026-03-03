package postgres

const (
	getPreferencesQuery = `
		SELECT id, email, email_notifications_enabled
		FROM auth.users
		WHERE id = $1
	`

	getUsernameQuery = `
		SELECT username
		FROM core.profiles
		WHERE user_id = $1
	`

	isProcessedQuery = `
		SELECT EXISTS (
			SELECT 1 FROM notification.processed_events WHERE event_id = $1
		)
	`

	markProcessedQuery = `
		INSERT INTO notification.processed_events (event_id)
		VALUES ($1)
		ON CONFLICT (event_id) DO NOTHING
	`
)
