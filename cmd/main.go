package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"

	"github.com/kuromii5/notification-service/config"
	kafkaconsumer "github.com/kuromii5/notification-service/internal/adapters/kafka"
	emailadapter "github.com/kuromii5/notification-service/internal/adapters/email"
	pgadapter "github.com/kuromii5/notification-service/internal/adapters/postgres"
	"github.com/kuromii5/notification-service/internal/service/notification"
)

func main() {
	cfg := config.MustLoad()
	setupLogger(cfg.Log.Level)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

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

	emailSender := emailadapter.NewSMTPSender(emailadapter.Config{
		Host:     cfg.SMTP.Host,
		Port:     cfg.SMTP.Port,
		Username: cfg.SMTP.Username,
		Password: cfg.SMTP.Password,
		From:     cfg.SMTP.From,
	})

	svc := notification.NewService(pg, emailSender, pg)

	consumer := kafkaconsumer.NewConsumer(kafkaconsumer.Config{
		Brokers: cfg.Kafka.Brokers,
		GroupID: cfg.Kafka.GroupID,
		Topic:   cfg.Kafka.Topic,
	}, svc, pg)
	defer func() {
		if err := consumer.Close(); err != nil {
			logrus.WithError(err).Error("Kafka consumer close failed")
		}
	}()

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
}
