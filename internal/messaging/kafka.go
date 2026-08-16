package messaging

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"notification-svc/internal/config"
)

type consumer struct {
	source  Source
	handler HandlerFunc
}

// KafkaImpl is the Kafka counterpart of RabbitImpl. notification-svc only
// consumes, so there is no writer here.
//
// Topology mapping: exchange -> topic (`billing`, `auth`), routing key -> the
// `event_type` field of the payload (the `billing.payment.*` wildcard becomes
// a client-side filter), queue -> consumer group.
type KafkaImpl struct {
	cfg       config.KafkaConfig
	consumers []consumer
}

func NewKafkaImpl(cfg config.KafkaConfig) *KafkaImpl {
	return &KafkaImpl{cfg: cfg}
}

func (k *KafkaImpl) GetNotificationSource() Source {
	return Source{
		Name:       k.cfg.BillingTopic,
		Group:      k.cfg.BillingGroup,
		EventTypes: k.cfg.BillingEventTypes,
	}
}

func (k *KafkaImpl) GetAuthSource() Source {
	return Source{
		Name:       k.cfg.AuthTopic,
		Group:      k.cfg.AuthGroup,
		EventTypes: k.cfg.AuthEventTypes,
	}
}

func (k *KafkaImpl) RegisterConsumer(s Source, h HandlerFunc) {
	k.consumers = append(k.consumers, consumer{source: s, handler: h})
}

func (k *KafkaImpl) Run() {
	wg := &sync.WaitGroup{}
	for _, c := range k.consumers {
		wg.Add(1)
		go k.runConsumer(c, wg)
	}
	wg.Wait()
}

func (k *KafkaImpl) runConsumer(c consumer, wg *sync.WaitGroup) {
	defer wg.Done()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: k.cfg.Brokers,
		Topic:   c.source.Name,
		GroupID: c.source.Group,
		// Manual commits: commit only after the handler succeeded, so a crash
		// leads to redelivery instead of a lost notification (at-least-once).
		CommitInterval: 0,
		MaxWait:        time.Second,
	})
	defer r.Close()

	slog.Info("kafka consumer started",
		"topic", c.source.Name, "group", c.source.Group, "event_types", c.source.EventTypes)

	for {
		msg, err := r.FetchMessage(context.Background())
		if err != nil {
			slog.Error("fetch message", "topic", c.source.Name, "err", err)
			return
		}

		if !matchesEventType(msg.Value, c.source.EventTypes) {
			// Not ours (the topic carries several event types): commit so we
			// do not re-read it forever.
			if err := r.CommitMessages(context.Background(), msg); err != nil {
				slog.Error("commit filtered message", "topic", c.source.Name, "err", err)
			}
			continue
		}

		ok, err := c.handler(msg.Value)
		if err != nil {
			// No commit -> redelivery. Handlers must be idempotent.
			slog.Error("handle message", "topic", c.source.Name, "offset", msg.Offset, "err", err)
			continue
		}
		if !ok {
			slog.Debug("message skipped", "topic", c.source.Name, "offset", msg.Offset)
		}

		if err := r.CommitMessages(context.Background(), msg); err != nil {
			slog.Error("commit message", "topic", c.source.Name, "offset", msg.Offset, "err", err)
		}
	}
}

// matchesEventType is the client-side replacement for AMQP routing-key
// wildcards such as `billing.payment.*`.
func matchesEventType(body []byte, allowed []string) bool {
	if len(allowed) == 0 {
		return true
	}

	var envelope struct {
		EventType string `json:"event_type"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		// Malformed payloads are handed to the handler, which reports the error.
		return true
	}

	for _, a := range allowed {
		if a == envelope.EventType {
			return true
		}
	}
	return false
}
