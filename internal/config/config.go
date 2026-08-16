package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
)

type PGConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string
	SSLMode  string
}

func (c PGConfig) DSN() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s",
		c.User, c.Password, c.Host, c.Port, c.Database, c.SSLMode,
	)
}

func GetPGConfig() PGConfig {
	return PGConfig{
		Host:     getenv("PG_HOST", "postgres"),
		Port:     getenv("PG_PORT", "5432"),
		User:     getenv("PG_USER", "billing"),
		Password: getenv("PG_PASSWORD", "billing"),
		Database: getenv("PG_DATABASE", "billing"),
		SSLMode:  getenv("PG_SSLMODE", "disable"),
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type RabbitConfig struct {
	DSN                       string
	BillingExchange           string
	AuthExchange              string
	NotificationConsumer      string
	AuthConsumer              string
	BillingConsumerRoutingKey string
	AuthConsumerRoutingKey    string
}

type BrokerConfig interface {
	GetNotificationConsumerSourceName() string
}

func (rc *RabbitConfig) GetNotificationConsumerSourceName() string {
	return rc.NotificationConsumer
}

// KafkaConfig mirrors RabbitConfig: exchanges become topics, routing keys
// become `event_type` values inside the payload (filtered client-side, since
// Kafka has no wildcard bindings), and queues become consumer groups.
type KafkaConfig struct {
	Brokers []string

	BillingTopic      string
	BillingGroup      string
	BillingEventTypes []string

	AuthTopic      string
	AuthGroup      string
	AuthEventTypes []string
}

type RestConfig struct {
}

type Config struct {
	BrokerType string
	Rest       RestConfig
}

func InitConfig() *Config {
	brokerType := os.Getenv("BROKER_TYPE")
	return &Config{BrokerType: brokerType, Rest: initRestConfig()}
}

func GetKafkaConfig() KafkaConfig {
	return KafkaConfig{
		Brokers: strings.Split(getenv("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092"), ","),

		BillingTopic: getenv("KAFKA_BILLING_TOPIC", "order-payment"),
		BillingGroup: getenv("KAFKA_BILLING_GROUP", "notificationsvc.billing"),
		// replaces the `payment.*` binding for the order-payment exchange
		BillingEventTypes: strings.Split(
			getenv("KAFKA_BILLING_EVENT_TYPES", "PAYMENT_SUCCESS,PAYMENT_FAILED"), ","),

		AuthTopic:      getenv("KAFKA_AUTH_TOPIC", "auth"),
		AuthGroup:      getenv("KAFKA_AUTH_GROUP", "notificationsvc.auth"),
		AuthEventTypes: strings.Split(getenv("KAFKA_AUTH_EVENT_TYPES", "user.created"), ","),
	}
}

func initRestConfig() RestConfig {
	return RestConfig{}
}

func GetRabbitConfig() RabbitConfig {
	user := os.Getenv("RABBIT_USERNAME")
	password := os.Getenv("RABBIT_PASSWORD")
	host := os.Getenv("RABBIT_HOST")
	port := os.Getenv("RABBIT_PORT")
	billingExchange := getenv("RABBIT_BILLING_EXCHANGE", "order-payment")
	authExchange := getenv("RABBIT_AUTH_EXCHANGE", "auth")
	notificationConsumer := getenv("RABBIT_NOTIFICATION_CONSUMER", "notificationsvc.consumer")
	authConsumer := getenv("RABBIT_AUTH_CONSUMER", "notificationsvc.auth.consumer")
	billingConsumerRoutingKey := getenv("RABBIT_BILLING_CONSUMER_ROUTING_KEY", "payment.*")
	authConsumerRoutingKey := getenv("RABBIT_AUTH_CONSUMER_ROUTING_KEY", "user.created")

	u := url.URL{Scheme: "amqp",
		User: url.UserPassword(user, password),
		Host: fmt.Sprintf("%s:%s", host, port)}

	return RabbitConfig{DSN: u.String(),
		BillingExchange:           billingExchange,
		AuthExchange:              authExchange,
		NotificationConsumer:      notificationConsumer,
		AuthConsumer:              authConsumer,
		BillingConsumerRoutingKey: billingConsumerRoutingKey,
		AuthConsumerRoutingKey:    authConsumerRoutingKey,
	}
}
