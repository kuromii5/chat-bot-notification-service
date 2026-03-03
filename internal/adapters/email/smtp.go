package email

import (
	"context"
	"fmt"
	"net/smtp"

	"github.com/kuromii5/notification-service/internal/domain"
)

type Config struct {
	Host     string
	Port     string
	Username string
	Password string
	From     string
}

type SMTPSender struct {
	cfg  Config
	auth smtp.Auth
	addr string
}

func NewSMTPSender(cfg Config) *SMTPSender {
	auth := smtp.PlainAuth("", cfg.Username, cfg.Password, cfg.Host)
	return &SMTPSender{
		cfg:  cfg,
		auth: auth,
		addr: cfg.Host + ":" + cfg.Port,
	}
}

func (s *SMTPSender) Send(_ context.Context, to string, n domain.Notification) error {
	msg := fmt.Sprintf(
		"From: %s\r\nTo: %s\r\nSubject: %s\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n%s",
		s.cfg.From, to, n.Subject, n.Body,
	)

	if err := smtp.SendMail(s.addr, s.auth, s.cfg.From, []string{to}, []byte(msg)); err != nil {
		return fmt.Errorf("smtp send: %w", err)
	}
	return nil
}
