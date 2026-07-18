package service

import (
	"context"
	"log/slog"

	"billing-svc/internal/models"
)

type NotificationService struct {
	store Store
}

func NewNotificationService(s Store) *NotificationService {
	return &NotificationService{store: s}
}

// CreateNotification persists a new order in `pending` status and publishes an
// `order.created` event so billing-svc can attempt to withdraw the money.
func (o *NotificationService) CreateNotification(ctx context.Context, notification models.Notification) (bool, error) {
	saved, err := o.store.CreateNotification(ctx, notification)
	if err != nil {
		return false, err
	}

	slog.Info("notification stored", "notification", saved)
	return true, nil
}

// ListOrders returns all orders for the given user.
func (o *NotificationService) ListOrders(ctx context.Context, userId int64) ([]models.Notification, error) {
	return o.store.ListNotificationsByUser(ctx, userId)
}
