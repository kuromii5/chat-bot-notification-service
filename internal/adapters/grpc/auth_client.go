package grpc

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	authv1 "github.com/kuromii5/chat-bot-shared/proto/auth/v1"
	"github.com/kuromii5/notification-service/internal/domain"
)

type AuthClient struct {
	client authv1.UserServiceClient
}

func NewAuthClient(addr string) (*AuthClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to auth-service: %w", err)
	}
	return &AuthClient{client: authv1.NewUserServiceClient(conn)}, nil
}

func (c *AuthClient) GetPreferences(ctx context.Context, userID uuid.UUID) (*domain.UserPreferences, error) {
	resp, err := c.client.GetPreferences(ctx, &authv1.GetPreferencesRequest{
		UserId: userID.String(),
	})
	if err != nil {
		return nil, fmt.Errorf("get preferences via grpc: %w", err)
	}

	return &domain.UserPreferences{
		UserID:                    userID,
		Email:                     resp.Email,
		EmailNotificationsEnabled: resp.EmailNotificationsEnabled,
	}, nil
}
