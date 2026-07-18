package config

import (
	"fmt"
	"net/url"
	"os"
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
	NotificationConsumer      string
	BillingConsumerRoutingKey string
}

type BrokerConfig interface {
	GetNotificationConsumerSourceName() string
}

func (rc *RabbitConfig) GetNotificationConsumerSourceName() string {
	return rc.NotificationConsumer
}

type KafkaConfig struct {
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
	panic("unimplemented")
}

func initRestConfig() RestConfig {
	return RestConfig{}
}

func GetRabbitConfig() RabbitConfig {
	user := os.Getenv("RABBIT_USERNAME")
	password := os.Getenv("RABBIT_PASSWORD")
	host := os.Getenv("RABBIT_HOST")
	port := os.Getenv("RABBIT_PORT")
	billingExchange := getenv("RABBIT_BILLING_EXCHANGE", "billing")
	notificationConsumer := getenv("RABBIT_NOTIFICATION_CONSUMER", "notificationsvc.consumer")
	billingConsumerRoutingKey := getenv("RABBIT_BILLING_CONSUMER_ROUTING_KEY", "billing.payment.*")

	u := url.URL{Scheme: "amqp",
		User: url.UserPassword(user, password),
		Host: fmt.Sprintf("%s:%s", host, port)}

	return RabbitConfig{DSN: u.String(),
		BillingExchange:           billingExchange,
		NotificationConsumer:      notificationConsumer,
		BillingConsumerRoutingKey: billingConsumerRoutingKey,
	}
}
