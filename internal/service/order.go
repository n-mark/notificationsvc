package service

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"notification-svc/internal/models"
)

type NotificationService struct {
	store Store
}

func NewNotificationService(s Store) *NotificationService {
	return &NotificationService{store: s}
}


func (o *NotificationService) CreateNotification(ctx context.Context, notification models.Notification) (bool, error) {
	saved, err := o.store.CreateNotification(ctx, notification)
	if err != nil {
		return false, err
	}

	slog.Info("notification stored", "notification", saved)
	return true, nil
}

// ProcessOrderPaymentEvent handles PAYMENT_SUCCESS/FAILED events from billing-svc.
// It stores a notification record per event.
func (o *NotificationService) ProcessOrderPaymentEvent(ctx context.Context, event models.OrderPaymentEvent) (bool, error) {
	if event.EventId == uuid.Nil {
		return false, fmt.Errorf("missing event id")
	}

	alreadyProcessed, err := o.store.EventAlreadyProcessed(ctx, event.EventId)
	if err != nil {
		return false, fmt.Errorf("check event processed: %w", err)
	}
	if alreadyProcessed {
		slog.Info("event already processed", "event_id", event.EventId)
		return true, nil
	}

	notification := models.Notification{
		ID:      event.EventId,
		UserId:  event.UserId,
		Message: event.Message,
		Type:    event.EventType,
	}
	if _, err := o.store.CreateNotification(ctx, notification); err != nil {
		return false, fmt.Errorf("store notification: %w", err)
	}

	if err := o.store.MarkEventProcessed(ctx, event.EventId, event.EventType); err != nil {
		return false, fmt.Errorf("mark event processed: %w", err)
	}

	return true, nil
}

// ProcessUserCreatedEvent handles the user.created event from auth-service.
// It stores a notification record and logs/sends the email message.
func (o *NotificationService) ProcessUserCreatedEvent(ctx context.Context, event models.UserCreatedEvent) (bool, error) {
	if event.EventId == uuid.Nil {
		return false, fmt.Errorf("missing event id")
	}

	alreadyProcessed, err := o.store.EventAlreadyProcessed(ctx, event.EventId)
	if err != nil {
		return false, fmt.Errorf("check event processed: %w", err)
	}
	if alreadyProcessed {
		slog.Info("event already processed", "event_id", event.EventId)
		return true, nil
	}

	notification := models.Notification{
		ID:      event.EventId,
		UserId:  0, // auth-service does not send user id in the event
		Message: event.Message,
		Type:    event.NotificationType,
	}
	if _, err := o.store.CreateNotification(ctx, notification); err != nil {
		return false, fmt.Errorf("store notification: %w", err)
	}

	// TODO: replace with real email sender integration
	slog.Info("sending email notification",
		"event_id", event.EventId,
		"email", event.Email,
		"message", event.Message,
	)

	if err := o.store.MarkEventProcessed(ctx, event.EventId, event.EventType); err != nil {
		return false, fmt.Errorf("mark event processed: %w", err)
	}

	return true, nil
}

// ListOrders returns all orders for the given user.
func (o *NotificationService) ListOrders(ctx context.Context, userId int64) ([]models.Notification, error) {
	return o.store.ListNotificationsByUser(ctx, userId)
}
