package ver1

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"

	"notification-svc/internal/models"
	"notification-svc/internal/service"

	"github.com/gin-gonic/gin"
)

type OrderHandler struct {
	notification service.NotificationService
}

func NewOrderHandler(order service.NotificationService) *OrderHandler {
	return &OrderHandler{notification: order}
}

// --- broker-driven handlers (signature is dictated by messaging.HandlerFunc) ---

func (h *OrderHandler) PostNotification(body []byte) (bool, error) {
	payload := models.OrderPaymentEvent{}
	if err := json.Unmarshal(body, &payload); err != nil {
		slog.Error("failed to unmarshal order payment event", "error", err)
		return false, err
	}

	return h.notification.ProcessOrderPaymentEvent(context.Background(), payload)
}

func (h *OrderHandler) PostUserCreated(body []byte) (bool, error) {
	payload := models.UserCreatedEvent{}
	if err := json.Unmarshal(body, &payload); err != nil {
		slog.Error("failed to unmarshal user.created event", "error", err)
		return false, err
	}

	return h.notification.ProcessUserCreatedEvent(context.Background(), payload)
}

// --- HTTP handlers (Gin) ---

func parseUserID(c *gin.Context) (int64, bool) {
	s := c.GetHeader("x-user-id")
	if s == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing x-user-id header"})
		return 0, false
	}
	id, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "x-user-id must be an integer"})
		return 0, false
	}
	return id, true
}

// ListNotificationsHandler handles GET /api/v1/order (all orders for x-user-id).
func (h *OrderHandler) ListNotificationsHandler(c *gin.Context) {
	userId, ok := parseUserID(c)
	if !ok {
		return
	}

	orders, err := h.notification.ListOrders(c.Request.Context(), userId)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, orders)
}
