package store

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"billing-svc/internal/models"
)

var (
	ErrNotFound = errors.New("order not found")
)

type PgStore struct {
	db *pgxpool.Pool
}

func NewPgStore(db *pgxpool.Pool) *PgStore {
	return &PgStore{db: db}
}

// CreateNotification inserts a new order and returns the persisted row.
func (p *PgStore) CreateNotification(ctx context.Context, o models.Notification) (models.Notification, error) {
	if o.ID == uuid.Nil {
		o.ID = uuid.New()
	}

	row := p.db.QueryRow(ctx, `
		INSERT INTO notifications (id, user_id, message, type)
		VALUES ($1, $2, $3, $4)
		RETURNING id, user_id, message, type, created_at, updated_at
	`, o.ID, o.UserId, o.Message, o.Type)

	out := models.Notification{}
	if err := row.Scan(&out.ID, &out.UserId, &out.Message, &out.Type, &out.CreatedAt, &out.UpdatedAt); err != nil {
		return models.Notification{}, err
	}
	return out, nil
}

// ListOrdersByUser returns all orders belonging to the given user, newest first.
func (p *PgStore) ListNotificationsByUser(ctx context.Context, userId int64) ([]models.Notification, error) {
	rows, err := p.db.Query(ctx, `
		SELECT id, user_id, message, type, created_at, updated_at
		FROM notifications
		WHERE user_id = $1
		ORDER BY created_at DESC
	`, userId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]models.Notification, 0)
	for rows.Next() {
		var o models.Notification
		if err := rows.Scan(&o.ID, &o.UserId, &o.Message, &o.Type, &o.CreatedAt, &o.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

func (p *PgStore) EventAlreadyProcessed(ctx context.Context, eventID uuid.UUID) (bool, error) {
	var n int
	err := p.db.QueryRow(ctx, `SELECT 1 FROM processed_events WHERE event_id = $1`, eventID).Scan(&n)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (p *PgStore) MarkEventProcessed(ctx context.Context, eventID uuid.UUID, eventType string) error {
	_, err := p.db.Exec(ctx, `
		INSERT INTO processed_events (event_id, event_type)
		VALUES ($1, $2)
		ON CONFLICT (event_id) DO NOTHING
	`, eventID, eventType)
	return err
}
