package internal

import (
	"context"
	"log"
	"time"

	"github.com/angusgyoung/gox/pkg"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const selectLatestPendingEventSql = `
	SELECT * FROM outbox WHERE status = $1
	ORDER BY timestamp desc
	LIMIT 1
`

const updateEventStatus = `
	UPDATE outbox SET status = $1, timestamp = $2
	WHERE id = $3
`

type Operator interface {
	PublishPending(ctx context.Context) (*pkg.Event, error)
}

type operator struct {
	pool      *pgxpool.Pool
	publisher *kafka.Producer
}

func (o *operator) PublishPending(ctx context.Context) (*pkg.Event, error) {
	conn, err := o.pool.Acquire(ctx)
	if err != nil {
		log.Printf("Failed to aquire connection from pool: %s\n", err)
		return nil, err
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		log.Printf("Failed to start transaction: %s\n", err)
		return nil, err
	}
	defer tx.Rollback(ctx)

	event := pkg.Event{}
	err = tx.QueryRow(ctx, selectLatestPendingEventSql, pkg.PENDING).Scan(
		&event.ID,
		&event.Timestamp,
		&event.Status,
		&event.Topic,
		&event.Key,
		&event.Message,
	)
	if err != nil {
		if err != pgx.ErrNoRows {
			log.Printf("Failed to query for pending events: %s\n", err)
		}
		return nil, err
	}

	delivery_chan := make(chan kafka.Event, 10000)
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &event.Topic,
			Partition: kafka.PartitionAny,
		},
		Value: event.Message,
	}
	err = o.publisher.Produce(message, delivery_chan)
	if err != nil {
		log.Printf("Failed to publish event: %s\n", err)
		return nil, err
	}

	e := <-delivery_chan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		log.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
		return nil, err
	} else {
		log.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}
	close(delivery_chan)

	event.Status = pkg.SENT.String()
	event.Timestamp = time.Now()

	_, err = tx.Exec(ctx, updateEventStatus, event.Status, event.Timestamp, event.ID)
	if err != nil {
		log.Printf("Failed to update event status: %s\n", err)
		return nil, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		log.Printf("Failed to commit transaction: %s\n", err)
		return nil, err
	}

	return &event, nil
}

func NewOperator(ctx context.Context) Operator {
	dbUrl := pkg.GetEnv("DATABASE_URL", "postgres://localhost:5432/outbox")
	kafkaBootstrapServers := pkg.GetEnv("BOOTSTRAP_URLS", "localhost:9092")

	pool, err := pgxpool.New(ctx, dbUrl)
	if err != nil {
		log.Fatalf("Unable to create connection pool: %s\n", err)
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrapServers,
		"client.id":         uuid.New().String(),
		"acks":              "all",
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %s\n", err)
	}

	return &operator{
		pool,
		producer,
	}
}
