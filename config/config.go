package config

import (
	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type Config struct {
	Database DatabaseConfig
	Kafka    KafkaConfig
	SMTP     SMTPConfig
	Log      LogConfig
}

type DatabaseConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
	SSLMode  string
}

type KafkaConfig struct {
	Brokers []string
	GroupID string
	Topic   string
}

type SMTPConfig struct {
	Host     string
	Port     string
	Username string
	Password string
	From     string
}

type LogConfig struct {
	Level string
}

func Load() (*Config, error) {
	_ = godotenv.Load()
	viper.AutomaticEnv()

	cfg := &Config{
		Database: DatabaseConfig{
			Host:     viper.GetString("DB_HOST"),
			Port:     viper.GetString("DB_PORT"),
			User:     viper.GetString("DB_USER"),
			Password: viper.GetString("DB_PASSWORD"),
			DBName:   viper.GetString("DB_NAME"),
			SSLMode:  viper.GetString("DB_SSLMODE"),
		},
		Kafka: KafkaConfig{
			Brokers: viper.GetStringSlice("KAFKA_BROKERS"),
			GroupID: viper.GetString("KAFKA_GROUP_ID"),
			Topic:   viper.GetString("KAFKA_TOPIC"),
		},
		SMTP: SMTPConfig{
			Host:     viper.GetString("SMTP_HOST"),
			Port:     viper.GetString("SMTP_PORT"),
			Username: viper.GetString("SMTP_USERNAME"),
			Password: viper.GetString("SMTP_PASSWORD"),
			From:     viper.GetString("SMTP_FROM"),
		},
		Log: LogConfig{
			Level: viper.GetString("LOG_LEVEL"),
		},
	}
	return cfg, nil
}

func MustLoad() *Config {
	cfg, err := Load()
	if err != nil {
		logrus.Fatalf("Failed to load config: %v", err)
	}
	return cfg
}
