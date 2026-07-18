-- Orders: created by users, processed asynchronously via billing-svc
CREATE TABLE IF NOT EXISTS notifications (
    id           UUID PRIMARY KEY,
    user_id      BIGINT         NOT NULL,
    message       TEXT    NOT NULL,
    type VARCHAR NOT NULL,
    created_at   TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_notifications_user_id ON notifications(user_id);

-- Outbox of processed events for idempotency
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     UUID PRIMARY KEY,
    event_type   TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
