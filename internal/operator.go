package internal

import (
	"context"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/angusgyoung/gox/pkg"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Operator interface {
	PublishPending(ctx context.Context) error
	Close() error
}

type operator struct {
	instanceId   uuid.UUID
	pool         *pgxpool.Pool
	publisher    *kafka.Producer
	consumer     *kafka.Consumer
	pollInterval int
}

func NewOperator(ctx context.Context) Operator {
	intervalStr := GetEnv("GOX_POLL_INTERVAL", "100")
	interval, err := strconv.Atoi(intervalStr)
	if err != nil {
		log.Panicf("Failed to parse interval '%s' to an integer", intervalStr)
	}
	dbUrl := GetEnv("GOX_DB_URL", "postgres://localhost:5432/outbox")
	kafkaBootstrapServers := GetEnv("GOX_BROKER_URLS", "localhost:9092")
	topics := strings.Split(GetEnv("GOX_TOPICS", ""), ",")

	instanceId := uuid.New()

	pool, err := pgxpool.New(ctx, dbUrl)
	if err != nil {
		log.Fatalf("Failed to create connection pool: %s\n", err)
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		log.Fatalf("Failed to aqcuire connection from pool: %s\n", err)
	}
	defer conn.Release()

	_, err = conn.Exec(ctx, createTableSql)
	if err != nil {
		log.Fatalf("Failed create table: %s\n", err)
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrapServers,
		"client.id":         uuid.New().String(),
		"acks":              "all",
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %s\n", err)
	}

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrapServers,
		"client.id":         instanceId,
		"group.id":          "gox-partitioning-group",
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s\n", err)
	}

	consumer.SubscribeTopics(topics, nil)

	return &operator{
		instanceId,
		pool,
		producer,
		consumer,
		interval,
	}
}

func (o *operator) PublishPending(ctx context.Context) error {
	o.consumer.Poll(o.pollInterval)
	assignedPartitions := assignedPartitions(o.consumer)

	if len(assignedPartitions) == 0 {
		log.Printf("No partitions assigned")
		return nil
	}

	log.Printf("Assigned partitions: %v\n", assignedPartitions)

	conn, err := o.pool.Acquire(ctx)
	if err != nil {
		log.Printf("Failed to aquire connection from pool: %s\n", err)
		return err
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		log.Printf("Failed to start transaction: %s\n", err)
		return err
	}
	defer tx.Rollback(ctx)

	event := pkg.Event{}
	err = tx.QueryRow(
		ctx,
		selectLatestPendingEventSql,
		pkg.PENDING,
		assignedPartitions).Scan(
		&event.ID,
		&event.Timestamp,
		&event.Status,
		&event.Topic,
		&event.Partition,
		&event.Key,
		&event.Message,
		&event.InstanceID,
	)

	if err != nil {
		if err != pgx.ErrNoRows {
			log.Printf("Failed to query for pending events: %s\n", err)
		}
		return err
	}

	delivery_chan := make(chan kafka.Event, 10000)
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &event.Topic,
			Partition: event.Partition,
		},
		Value: event.Message,
	}
	err = o.publisher.Produce(message, delivery_chan)
	if err != nil {
		log.Printf("Failed to publish event: %s\n", err)
		return err
	}

	e := <-delivery_chan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		log.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
		return err
	} else {
		log.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}
	close(delivery_chan)

	event.Status = pkg.SENT.String()
	event.Timestamp = time.Now()

	_, err = tx.Exec(ctx,
		updateEventStatusSql,
		event.Status,
		event.Timestamp,
		o.instanceId,
		event.ID)
	if err != nil {
		log.Printf("Failed to update event status: %s\n", err)
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		log.Printf("Failed to commit transaction: %s\n", err)
		return err
	}

	return err
}

func (o *operator) Close() error {
	o.pool.Close()
	log.Println("Closed connection pool")
	o.publisher.Close()
	log.Println("Closed publisher")
	o.consumer.Close()
	log.Println("Closed consumer")
	return nil
}

func assignedPartitions(consumer *kafka.Consumer) []int {
	assignedTopicPartitions, err := consumer.Assignment()
	if err != nil {
		log.Fatalf("Failed to get assigned partitions: %s\n", err)
	}

	err = consumer.Pause(assignedTopicPartitions)
	if err != nil {
		log.Fatalf("Failed to pause consumer: %s\n", err)
	}

	assignedPartitions := make([]int, len(assignedTopicPartitions))
	for i := range assignedTopicPartitions {
		assignedPartitions[i] = int(assignedTopicPartitions[i].Partition)
	}

	return assignedPartitions
}
