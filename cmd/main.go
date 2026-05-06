package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"

	"github.com/kuromii5/chat-bot-shared/tracing"

	"github.com/kuromii5/notification-service/config"
	"github.com/kuromii5/notification-service/internal/app"
)

func main() {
	cfg := config.MustLoad()
	setupLogger(cfg.Log.Level)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	a, err := app.New(ctx, cfg)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to initialize app")
	}

	a.Run(ctx)

	logrus.Info("Shutting down...")
	a.Close(context.Background())
	logrus.Info("Notification service stopped")
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
