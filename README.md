# Notification Service (Сервис уведомлений)

Микросервис уведомлений для интернет-сервиса по размещению объявлений (финальный проект). Формирует и хранит уведомления пользователей по событиям из Kafka. Написан на Go.

- **GitHub:** https://github.com/n-mark/notificationsvc
- **DockerHub:** [`mblkuta/notificationsvc`](https://hub.docker.com/r/mblkuta/notificationsvc)

## Возможности

- Хранение уведомлений и мок-«входящие» (`/api/v1/notifications`, `/api/v1/notification/inbox_mock`)
- Подписка на события оплаты (`PAYMENT_SUCCESS`, `PAYMENT_FAILED`) из топика `order-payment`
- Подписка на события регистрации (`user.created`) из топика `auth` — приветственные уведомления

## Технологии

- Go, PostgreSQL, Kafka
- Docker / docker-compose

## Структура проекта

```text
main.go        # точка входа
internal/      # бизнес-логика, обработчики, хранилище
migrations/    # SQL-миграции
```

## Переменные окружения

| Переменная | Описание | Пример |
|---|---|---|
| `APP_PORT` | Порт HTTP-сервера | `8080` |
| `PG_HOST` / `PG_PORT` | Хост/порт PostgreSQL | `postgres-notifications` / `5432` |
| `PG_DATABASE` | Имя БД | `notificationdb` |
| `PG_USER` / `PG_PASSWORD` | Учётные данные БД | `notification_user` |
| `PG_SSLMODE` | Режим SSL | `disable` |
| `BROKER_TYPE` | Тип брокера | `KAFKA` |
| `KAFKA_BROKERS` | Адреса брокеров Kafka | `kafka:9092` |
| `KAFKA_BILLING_TOPIC` / `KAFKA_BILLING_GROUP` | Топик/группа оплат | `order-payment` / `notificationsvc.billing` |
| `KAFKA_BILLING_EVENT_TYPES` | Типы событий оплат | `PAYMENT_SUCCESS,PAYMENT_FAILED` |
| `KAFKA_AUTH_TOPIC` / `KAFKA_AUTH_GROUP` | Топик/группа аутентификации | `auth` / `notificationsvc.auth` |
| `KAFKA_AUTH_EVENT_TYPES` | Типы событий аутентификации | `user.created` |

## Запуск

### Docker Compose

```bash
docker compose up -d
```

### Локально

```bash
go run ./main.go
```

## Эндпоинты

- `GET /health` — health-check
- `/api/v1/notifications/...` — уведомления пользователя
- `/api/v1/notification/inbox_mock` — мок входящих

## Связанные репозитории

Инфраструктура всего проекта (k8s, Helm, docker-compose всего стека): https://github.com/n-mark/final-project