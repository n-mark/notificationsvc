package models

import (
	"time"

	"github.com/google/uuid"
)

type Notification struct {
	ID        uuid.UUID `json:"id"`
	UserId    int64     `json:"user_id"`
	Message   string    `json:"message"`
	Type      string    `json:"type"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// OrderPaymentEvent is emitted by billing-svc on the `order-payment` topic.
// It notifies about payment success/failure for an order.
type OrderPaymentEvent struct {
	EventId       uuid.UUID `json:"event_id"`
	EventType     string    `json:"event_type"`
	OrderId       uuid.UUID `json:"order_id"`
	UserId        int64     `json:"user_id"`
	TransactionId uuid.UUID `json:"transaction_id"`
	Status        string    `json:"status"`
	Message       string    `json:"message"`
}

// UserCreatedEvent is emitted by auth-service when a new user registers.
type UserCreatedEvent struct {
	EventId          uuid.UUID `json:"event_id"`
	EventType        string    `json:"event_type"`
	NotificationType string    `json:"notification_type"`
	Message          string    `json:"message"`
	Version          string    `json:"version"`
	Email            string    `json:"email"`
	Payload          string    `json:"payload"`
}
