package service

import (
	"context"

	"github.com/google/uuid"

	"billing-svc/internal/models"
)

type Store interface {
	CreateNotification(ctx context.Context, o models.Notification) (models.Notification, error)
	ListNotificationsByUser(ctx context.Context, userId int64) ([]models.Notification, error)

	EventAlreadyProcessed(ctx context.Context, eventID uuid.UUID) (bool, error)
	MarkEventProcessed(ctx context.Context, eventID uuid.UUID, eventType string) error
}
