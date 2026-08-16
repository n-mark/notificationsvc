package messaging

import (
	"notification-svc/internal/config"
	"fmt"
	"log/slog"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type HandlerFunc func(b []byte) (bool, error)

type RabbitImpl struct {
	conn          *amqp.Connection
	publisher     *amqp.Channel
	publisherLock sync.Mutex
	consumers     map[string]HandlerFunc
	cfg           config.RabbitConfig
}

// RabbitMQ filters server-side via queue bindings, so no EventTypes here.
func (r *RabbitImpl) GetNotificationSource() Source {
	return Source{Name: r.cfg.NotificationConsumer}
}

func (r *RabbitImpl) GetAuthSource() Source {
	return Source{Name: r.cfg.AuthConsumer}
}

func NewRabbitImpl(cfg config.RabbitConfig) (*RabbitImpl, error) {
	conn, err := amqp.Dial(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("connect to rabbitmq: %w", err)
	}

	publisher, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("open publisher channel: %w", err)
	}

	r := &RabbitImpl{conn: conn, consumers: make(map[string]HandlerFunc), cfg: cfg, publisher: publisher}

	return r, nil
}

// TODO: вот здесь
func (r *RabbitImpl) declareExchange(ch *amqp.Channel, exchange string) error {
	return ch.ExchangeDeclare(
		exchange,
		"topic",
		true,  // durable
		false, // auto-delete
		false, // internal
		false, // no-wait
		nil,
	)
}

func (r *RabbitImpl) declareQueueAndBind(ch *amqp.Channel, queue string) error {
	if _, err := ch.QueueDeclare(
		queue,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	); err != nil {
		return fmt.Errorf("declare queue %q: %w", queue, err)
	}

	exchange, rks := r.exchangeAndRoutingKeysFor(queue)
	if exchange == "" || len(rks) == 0 {
		return nil
	}

	for _, rk := range rks {
		if err := ch.QueueBind(queue, rk, exchange, false, nil); err != nil {
			return fmt.Errorf("bind queue %q to %q with rk %q: %w", queue, exchange, rk, err)
		}
	}

	return nil
}

func (r *RabbitImpl) exchangeAndRoutingKeysFor(queue string) (string, []string) {
	switch queue {
	case r.cfg.NotificationConsumer:
		return r.cfg.BillingExchange, []string{r.cfg.BillingConsumerRoutingKey}
	case r.cfg.AuthConsumer:
		return r.cfg.AuthExchange, []string{r.cfg.AuthConsumerRoutingKey}
	}
	return "", nil
}

func (r *RabbitImpl) RegisterConsumer(s Source, h HandlerFunc) {
	r.consumers[s.Name] = h
}

func (r *RabbitImpl) Run() {
	defer r.conn.Close()
	defer r.publisher.Close()

	exchangesToDeclare := []string{r.cfg.BillingExchange, r.cfg.AuthExchange}
	for _, exchange := range exchangesToDeclare {
		if exchange == "" {
			continue
		}
		if err := r.declareExchange(r.publisher, exchange); err != nil {
			slog.Error("declare topology", "op", "exchange", "err", err)
			return
		}
	}

	// Declare the entire topology up front, on the publisher channel, so
	// queues/exchange are guaranteed to exist before any consumer starts.
	for queue := range r.consumers {
		if err := r.declareQueueAndBind(r.publisher, queue); err != nil {
			slog.Error("declare topology", "queue", queue, "err", err)
			return
		}
		slog.Info("queue ready", "queue", queue)
	}

	wg := &sync.WaitGroup{}
	for k, v := range r.consumers {
		wg.Add(1)
		go r.runConsumer(k, v, wg)
	}
	wg.Wait()
}

func (r *RabbitImpl) runConsumer(queue string, handler HandlerFunc, wg *sync.WaitGroup) {
	defer wg.Done()
	ch, err := r.conn.Channel()
	if err != nil {
		slog.Error("create channel", "err", err)
		return
	}

	defer ch.Close()

	if err := ch.Qos(1, 0, false); err != nil {
		slog.Error("set qos", "err", err)
		return
	}

	msgs, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		slog.Error("consume message", "err", err)
		return
	}

	for msg := range msgs {
		ok, err := handler(msg.Body)
		if err != nil {
			msg.Nack(false, false)
			continue
		}

		if !ok {
			msg.Ack(false)
			continue
		}

		msg.Ack(false)
	}
}
