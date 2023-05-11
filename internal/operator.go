package internal

import (
	"context"
	"log"
	"time"

	"github.com/angusgyoung/gox/pkg"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type Operator interface {
	PublishPending(ctx context.Context) error
	Close(ctx context.Context)
}

type operator struct {
	instanceId uuid.UUID
	conn       *pgx.Conn
	publisher  *kafka.Producer
	consumer   *kafka.Consumer
	config     *OperatorConfig
}

type OperatorConfig struct {
	PollInterval int
	BatchSize    int
	DatabaseUrl  string
	BrokerUrls   string
	Topics       []string
}

func NewOperator(ctx context.Context, config *OperatorConfig) Operator {
	instanceId := uuid.New()

	conn, err := pgx.Connect(ctx, config.DatabaseUrl)
	if err != nil {
		log.Fatalf("Failed to acquire database connection: %s\n", err)
	}

	_, err = conn.Exec(ctx, createTableSql)
	if err != nil {
		log.Fatalf("Failed create table: %s\n", err)
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": config.BrokerUrls,
		"client.id":         uuid.New().String(),
		"acks":              "all",
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %s\n", err)
	}

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": config.BrokerUrls,
		"client.id":         instanceId,
		"group.id":          "gox-partitioning-group",
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s\n", err)
	}

	consumer.SubscribeTopics(config.Topics, nil)

	return &operator{
		instanceId,
		conn,
		producer,
		consumer,
		config,
	}
}

func (o *operator) PublishPending(ctx context.Context) error {
	o.consumer.Poll(o.config.PollInterval)
	assignedTopicPartitions := consumerAssignedTopicPartitions(o.consumer)

	if len(assignedTopicPartitions) == 0 {
		return nil
	}

	topicPartitions := make(map[string][]int)

	for _, topicPartition := range assignedTopicPartitions {
		topicPartitions[*topicPartition.Topic] = append(topicPartitions[*topicPartition.Topic], int(topicPartition.Partition))
	}

	for topic, partitions := range topicPartitions {
		tx, err := o.conn.Begin(ctx)
		if err != nil {
			log.Printf("Failed to start transaction: %s\n", err)
			return err
		}
		defer tx.Rollback(ctx)

		rows, err := tx.Query(
			ctx,
			selectLatestPendingEventsSql,
			pkg.PENDING,
			partitions,
			topic,
			o.config.BatchSize,
		)

		if err != nil {
			log.Printf("Failed to query for pending events: %s\n", err)
			return err
		}
		defer rows.Close()

		var eventIds []uuid.UUID
		deliveryChan := make(chan kafka.Event, 10000)

		for rows.Next() {
			event := pkg.Event{}

			err := rows.Scan(
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
				log.Printf("Failed to read row: %s\n", err)
				return err
			}

			message := &kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &event.Topic,
					Partition: event.Partition,
				},
				Key:   []byte(event.Key),
				Value: event.Message,
			}
			err = o.publisher.Produce(message, deliveryChan)
			if err != nil {
				log.Printf("Failed to publish event: %s\n", err)
				return err
			}

			e := <-deliveryChan
			m := e.(*kafka.Message)

			if m.TopicPartition.Error != nil {
				log.Printf("Delivery of message with key '%s' to topic '%s' '[%d]' failed: %v\n",
					m.Key, *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Error)
				return err
			} else {
				log.Printf("Delivered message with key '%s' to topic '%s' '[%d]' at offset '%v'\n",
					m.Key, *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
			}

			eventIds = append(eventIds, event.ID)
		}

		close(deliveryChan)

		// We didn't find any events to publish
		if len(eventIds) == 0 {
			return nil
		}

		_, err = tx.Exec(ctx,
			updateEventStatusSql,
			pkg.SENT.String(),
			time.Now(),
			o.instanceId,
			eventIds)
		if err != nil {
			log.Printf("Failed to update event status: %s\n", err)
			return err
		}

		err = tx.Commit(ctx)
		if err != nil {
			log.Printf("Failed to commit transaction: %s\n", err)
			return err
		}

		log.Printf("Sent %d events\n", len(eventIds))

	}

	return nil
}

func (o *operator) Close(ctx context.Context) {
	log.Println("Closing connections...")
	o.conn.Close(ctx)
	o.publisher.Close()
	o.consumer.Close()
}

func consumerAssignedTopicPartitions(consumer *kafka.Consumer) []kafka.TopicPartition {
	assignedTopicPartitions, err := consumer.Assignment()
	if err != nil {
		log.Fatalf("Failed to get assigned topic partitions: %s\n", err)
	}

	err = consumer.Pause(assignedTopicPartitions)
	if err != nil {
		log.Fatalf("Failed to pause consumer: %s\n", err)
	}
	return assignedTopicPartitions
}
