package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"

	"github.com/kuromii5/chat-bot-shared/tracing"

	"github.com/kuromii5/notification-service/config"
	emailadapter "github.com/kuromii5/notification-service/internal/adapters/email"
	grpcadapter "github.com/kuromii5/notification-service/internal/adapters/grpc"
	kafkaconsumer "github.com/kuromii5/notification-service/internal/adapters/kafka"
	pgadapter "github.com/kuromii5/notification-service/internal/adapters/postgres"
	tracingadapter "github.com/kuromii5/notification-service/internal/adapters/tracing"
	httphandlers "github.com/kuromii5/notification-service/internal/handlers/http"
	"github.com/kuromii5/notification-service/internal/service/notification"
	tracingsvc "github.com/kuromii5/notification-service/internal/service/tracing"
)

func main() {
	cfg := config.MustLoad()
	setupLogger(cfg.Log.Level)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	shutdownTracer, err := tracing.InitTracer(
		context.Background(),
		"notification-service",
		cfg.Tracing.Endpoint,
		cfg.Tracing.Sampler,
	)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to init OpenTelemetry")
	}
	defer func() {
		if err := shutdownTracer(context.Background()); err != nil {
			logrus.WithError(err).Error("Failed to shutdown tracer")
		}
	}()

	pg, err := pgadapter.New(pgadapter.Config{
		Host:     cfg.Database.Host,
		Port:     cfg.Database.Port,
		User:     cfg.Database.User,
		Password: cfg.Database.Password,
		DBName:   cfg.Database.DBName,
		SSLMode:  cfg.Database.SSLMode,
	})
	if err != nil {
		logrus.WithError(err).Fatal("Failed to connect to database")
	}
	defer func() {
		if err := pg.Close(); err != nil {
			logrus.WithError(err).Error("DB close failed")
		}
	}()

	authClient, err := grpcadapter.NewAuthClient(cfg.AuthGRPCAddr)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to connect to auth-service gRPC")
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

	consumer := kafkaconsumer.NewConsumer(kafkaconsumer.Config{
		Brokers: cfg.Kafka.Brokers,
		GroupID: cfg.Kafka.GroupID,
		Topic:   cfg.Kafka.Topic,
	}, tracingHandler)
	defer func() {
		if err := consumer.Close(); err != nil {
			logrus.WithError(err).Error("Kafka consumer close failed")
		}
	}()

	httphandlers.InitMetrics(ctx, cfg.Metrics.Port)

	logrus.Info("Notification service started")
	consumer.Run(ctx)
	logrus.Info("Notification service shutdown")
}

func setupLogger(level string) {
	lvl, err := logrus.ParseLevel(level)
	if err != nil {
		lvl = logrus.InfoLevel
	}
	logrus.SetLevel(lvl)
	logrus.SetFormatter(&logrus.TextFormatter{
		DisableTimestamp: true,
	})
	logrus.AddHook(&tracing.OTelHook{})
}
