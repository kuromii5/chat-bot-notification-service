package app

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/kuromii5/notification-service/config"
	emailadapter "github.com/kuromii5/notification-service/internal/adapters/email"
	grpcadapter "github.com/kuromii5/notification-service/internal/adapters/grpc"
	kafkaconsumer "github.com/kuromii5/notification-service/internal/adapters/kafka"
	pgadapter "github.com/kuromii5/notification-service/internal/adapters/postgres"
	tracingadapter "github.com/kuromii5/notification-service/internal/adapters/tracing"
	httphandlers "github.com/kuromii5/notification-service/internal/handlers/http"
	"github.com/kuromii5/notification-service/internal/service/notification"
	tracingsvc "github.com/kuromii5/notification-service/internal/service/tracing"
	"github.com/kuromii5/chat-bot-shared/tracing"
)

type App struct {
	closer   Closer
	consumer *kafkaconsumer.Consumer
}

func New(ctx context.Context, cfg *config.Config) (*App, error) {
	var a App

	shutdownTracer, err := tracing.InitTracer(
		context.Background(),
		"notification-service",
		cfg.Tracing.Endpoint,
		cfg.Tracing.Sampler,
	)
	if err != nil {
		return nil, fmt.Errorf("init tracer: %w", err)
	}
	a.closer.Add(shutdownTracer)

	pg, err := pgadapter.New(pgadapter.Config{
		Host:     cfg.Database.Host,
		Port:     cfg.Database.Port,
		User:     cfg.Database.User,
		Password: cfg.Database.Password,
		DBName:   cfg.Database.DBName,
		SSLMode:  cfg.Database.SSLMode,
	})
	if err != nil {
		return nil, fmt.Errorf("connect to database: %w", err)
	}
	a.closer.Add(func(_ context.Context) error { return pg.Close() })

	authClient, err := grpcadapter.NewAuthClient(cfg.AuthGRPCAddr)
	if err != nil {
		return nil, fmt.Errorf("connect to auth-service gRPC: %w", err)
	}

	tracingPG := tracingadapter.NewRepo(pg)

	emailSender := emailadapter.NewSMTPSender(emailadapter.Config{
		Host:     cfg.SMTP.Host,
		Port:     cfg.SMTP.Port,
		Username: cfg.SMTP.Username,
		Password: cfg.SMTP.Password,
		From:     cfg.SMTP.From,
	})
	tracingEmail := tracingadapter.NewEmailSender(emailSender)

	svc := notification.NewService(authClient, tracingPG, tracingEmail, tracingPG)
	tracingSvc := tracingsvc.NewNotificationService(svc)

	eventHandler := kafkaconsumer.NewEventHandler(tracingSvc)
	tracingHandler := tracingadapter.NewKafka(eventHandler)

	a.consumer = kafkaconsumer.NewConsumer(kafkaconsumer.Config{
		Brokers: cfg.Kafka.Brokers,
		GroupID: cfg.Kafka.GroupID,
		Topic:   cfg.Kafka.Topic,
	}, tracingHandler)
	a.closer.Add(func(_ context.Context) error { return a.consumer.Close() })

	httphandlers.InitMetrics(ctx, cfg.Metrics.Port)

	return &a, nil
}

func (a *App) Run(ctx context.Context) {
	logrus.Info("Notification service started")
	a.consumer.Run(ctx)
}

func (a *App) Close(ctx context.Context) {
	shutdownCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	a.closer.Close(shutdownCtx)
}
