package internal

import (
	"context"
	"log"
	"time"

	"github.com/angusgyoung/gox/pkg"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type Operator interface {
	Execute(ctx context.Context) error
	Close(ctx context.Context)
}

type operator struct {
	instanceId uuid.UUID
	conn       conn
	producer   producer
	consumer   consumer
	config     *OperatorConfig
}

type conn interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Close(ctx context.Context) error
}

type producer interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Close()
}

type consumer interface {
	Poll(timeoutMs int) (event kafka.Event)
	SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) (err error)
	Assignment() (partitions []kafka.TopicPartition, err error)
	Pause(partitions []kafka.TopicPartition) (err error)
	Close() error
}

type scannableRow interface {
	Scan(dest ...any) error
}

type OperatorConfig struct {
	// Interval between consumer polls
	PollInterval int
	// Number of events to attempt to fetch on each
	// interval
	BatchSize int
	// JDBC connection url
	DatabaseUrl string
	// Comma-separated set of bootstrap urls
	BrokerUrls string
	// List of topics to subscribe to
	Topics []string
}

func NewOperator(ctx context.Context, config *OperatorConfig) (Operator, error) {
	instanceId := uuid.New()

	conn, err := pgx.Connect(ctx, config.DatabaseUrl)
	if err != nil {
		log.Printf("Failed to acquire database connection: %s\n", err)
		return nil, err
	}

	_, err = conn.Exec(ctx, createTableSql)
	if err != nil {
		log.Printf("Failed create table: %s\n", err)
		return nil, err
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": config.BrokerUrls,
		"client.id":         instanceId,
		"acks":              "all",
	})
	if err != nil {
		log.Printf("Failed to create producer: %s\n", err)
		return nil, err
	}

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": config.BrokerUrls,
		"client.id":         instanceId,
		"group.id":          "gox-partitioning-group",
	})
	if err != nil {
		log.Printf("Failed to create consumer: %s\n", err)
		return nil, err
	}

	err = consumer.SubscribeTopics(config.Topics, rebalanceCallback)
	if err != nil {
		log.Printf("Failed to subscribe to topics: %s\n", err)
		return nil, err
	}

	return &operator{
		instanceId,
		conn,
		producer,
		consumer,
		config,
	}, nil
}

func (o *operator) Execute(ctx context.Context) error {
	// Poll the consumer. We should then receive any assignment
	// updates which we should use to fetch events from the table
	o.consumer.Poll(o.config.PollInterval)

	assignedTopicPartitions, err := o.consumer.Assignment()
	if err != nil {
		log.Fatalf("Failed to get assigned topic partitions: %s\n", err)
	}

	// Pause our consumer against the partitions it has been assigned
	err = o.consumer.Pause(assignedTopicPartitions)
	if err != nil {
		log.Printf("Failed to pause consumer: %s\n", err)
		return err
	}

	// If we haven't been assigned any partitions we can return early
	if len(assignedTopicPartitions) == 0 {
		return nil
	}

	// Flatten our assigned partitions into a map of topic-> partitions[]
	topicPartitions := make(map[string][]int)
	for _, topicPartition := range assignedTopicPartitions {
		topicPartitions[*topicPartition.Topic] = append(topicPartitions[*topicPartition.Topic],
			int(topicPartition.Partition))
	}

	// Start our transaction
	tx, err := o.conn.Begin(ctx)
	if err != nil {
		log.Printf("Failed to start transaction: %s\n", err)
		return err
	}
	defer tx.Rollback(ctx)

	// Query for events where the status is 'PENDING', and the topic
	// and partition has been assigned by the consumer group to this instance
	rows, err := tx.Query(
		ctx,
		buildFetchPendingEventsQuery(topicPartitions),
		pkg.PENDING,
		o.config.BatchSize,
	)

	if err != nil {
		log.Printf("Failed to query for pending events: %s\n", err)
		return err
	}
	defer rows.Close()

	var eventIds []uuid.UUID
	// Create a channel to produce events into
	deliveryChan := make(chan kafka.Event, o.config.BatchSize)

	for rows.Next() {
		// Create an event from the row
		event, err := constructEvent(rows)
		if err != nil {
			log.Printf("Failed to construct event: %s\n", err)
		}
		// Create a message from the event
		message := constructMessage(*event)

		// Publish our message to the channel
		err = o.producer.Produce(message, deliveryChan)
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

		// Add the event ID's that we have published to our slice
		eventIds = append(eventIds, event.ID)
	}

	close(deliveryChan)

	// We didn't find any events to publish
	if len(eventIds) == 0 {
		return nil
	}

	// Update all of the events that we have published with the
	// status 'SENT', the current timestamp and our instance ID
	_, err = tx.Exec(ctx,
		updateEventStatusSql,
		pkg.SENT.String(),
		time.Now().UTC(),
		o.instanceId,
		eventIds)
	if err != nil {
		log.Printf("Failed to update event status: %s\n", err)
		return err
	}

	// Commit the transaction
	err = tx.Commit(ctx)
	if err != nil {
		log.Printf("Failed to commit transaction: %s\n", err)
		return err
	}

	log.Printf("Published %d event(s)\n", len(eventIds))

	return nil
}

func (o *operator) Close(ctx context.Context) {
	log.Println("Closing connections...")
	o.conn.Close(ctx)
	o.producer.Close()
	o.consumer.Close()
}

func constructEvent(row scannableRow) (*pkg.Event, error) {
	event := pkg.Event{}

	err := row.Scan(
		&event.ID,
		&event.CreatedTimestamp,
		&event.UpdatedTimestamp,
		&event.Status,
		&event.Topic,
		&event.Partition,
		&event.Key,
		&event.Message,
		&event.InstanceID,
	)

	return &event, err
}

func constructMessage(event pkg.Event) *kafka.Message {
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &event.Topic,
			Partition: event.Partition,
		},
		Key:   []byte(event.Key),
		Value: event.Message,
	}

	return message
}

func rebalanceCallback(c *kafka.Consumer, event kafka.Event) error {

	switch ev := event.(type) {
	case kafka.AssignedPartitions:
		log.Printf("%s rebalance: %d new partition(s) assigned\n",
			c.GetRebalanceProtocol(), len(ev.Partitions))

	case kafka.RevokedPartitions:
		log.Printf("%s rebalance: %d partition(s) revoked\n",
			c.GetRebalanceProtocol(), len(ev.Partitions))
		if c.AssignmentLost() {
			log.Printf("Current assignment lost\n")
		}
	}

	return nil
}
