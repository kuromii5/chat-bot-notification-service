//go:build integration

package integration

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	pgadapter "github.com/kuromii5/notification-service/internal/adapters/postgres"
)

const (
	testDBName   = "test_notification"
	testUser     = "test"
	testPassword = "test"
)

var (
	testDB   *sqlx.DB
	testRepo *pgadapter.DB
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	container, connStr, err := startPostgres(ctx)
	if err != nil {
		log.Fatalf("start postgres container: %v", err)
	}

	testDB, err = sqlx.Connect("pgx", connStr)
	if err != nil {
		log.Fatalf("connect to test db: %v", err)
	}

	if err := applyMigrations(testDB); err != nil {
		log.Fatalf("apply migrations: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		log.Fatalf("get container host: %v", err)
	}
	mappedPort, err := container.MappedPort(ctx, "5432")
	if err != nil {
		log.Fatalf("get mapped port: %v", err)
	}

	testRepo, err = pgadapter.New(pgadapter.Config{
		Host:     host,
		Port:     mappedPort.Port(),
		User:     testUser,
		Password: testPassword,
		DBName:   testDBName,
		SSLMode:  "disable",
	})
	if err != nil {
		log.Fatalf("create postgres adapter: %v", err)
	}

	code := m.Run()

	testDB.Close()
	container.Terminate(ctx)
	os.Exit(code)
}

func startPostgres(ctx context.Context) (testcontainers.Container, string, error) {
	container, err := postgres.Run(ctx,
		"postgres:18-alpine",
		postgres.WithDatabase(testDBName),
		postgres.WithUsername(testUser),
		postgres.WithPassword(testPassword),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		return nil, "", fmt.Errorf("run postgres container: %w", err)
	}

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, "", fmt.Errorf("get connection string: %w", err)
	}

	return container, connStr, nil
}

func applyMigrations(db *sqlx.DB) error {
	// 001 and 002 use env var placeholders — create schemas manually instead
	for _, schema := range []string{"auth", "core", "notification"} {
		if _, err := db.Exec("CREATE SCHEMA IF NOT EXISTS " + schema); err != nil {
			return fmt.Errorf("create schema %s: %w", schema, err)
		}
	}

	migrationsDir := filepath.Join("..", "..", "..", "migrations", "migrations")
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sql" {
			continue
		}
		// skip 001 and 002 — handled above
		if entry.Name() < "003" {
			continue
		}

		path := filepath.Join(migrationsDir, entry.Name())
		sql, err := extractUpSQL(path)
		if err != nil {
			return fmt.Errorf("extract up sql from %s: %w", entry.Name(), err)
		}
		if _, err := db.Exec(sql); err != nil {
			return fmt.Errorf("exec migration %s: %w", entry.Name(), err)
		}
	}

	return nil
}

func extractUpSQL(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	content := string(data)

	upIdx := strings.Index(content, "-- +migrate Up")
	if upIdx == -1 {
		return "", fmt.Errorf("no '-- +migrate Up' marker in %s", path)
	}
	content = content[upIdx+len("-- +migrate Up"):]

	if downIdx := strings.Index(content, "-- +migrate Down"); downIdx != -1 {
		content = content[:downIdx]
	}

	content = strings.ReplaceAll(content, "-- +migrate StatementBegin", "")
	content = strings.ReplaceAll(content, "-- +migrate StatementEnd", "")

	return strings.TrimSpace(content), nil
}

func truncateAll(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		testDB.MustExec("TRUNCATE notification.processed_events, core.profiles, auth.users CASCADE")
	})
}

func createTestUser(t *testing.T, email string, notificationsEnabled bool) (userID string, username string) {
	t.Helper()

	username = "user_" + strings.ReplaceAll(email, "@", "_")
	err := testDB.QueryRow(`
		INSERT INTO auth.users (email, password_hash, role, email_notifications_enabled)
		VALUES ($1, 'hashed', 'Human', $2)
		RETURNING id::text
	`, email, notificationsEnabled).Scan(&userID)
	if err != nil {
		t.Fatalf("create test user: %v", err)
	}

	_, err = testDB.Exec(`
		INSERT INTO core.profiles (user_id, username)
		VALUES ($1, $2)
	`, userID, username)
	if err != nil {
		t.Fatalf("create test profile: %v", err)
	}

	return userID, username
}
