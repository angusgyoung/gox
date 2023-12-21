package operator

import (
	"context"
	"github.com/angusgyoung/gox/internal/migrations"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/angusgyoung/gox/pkg"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var (
	publishedMessageCounter metric.Int64Counter
	rebalanceEventCounter   metric.Int64Counter
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

type OperatorConfig struct {
	// Interval between consumer polls
	PollInterval int
	// Number of events to attempt to fetch on each
	// interval
	BatchSize int
	// Posgres connection url
	DatabaseUrl string
	// Comma-separated set of bootstrap urls
	BrokerUrls string
	// List of topics to subscribe to
	Topics []string
}

func NewOperator(ctx context.Context, config *OperatorConfig) (Operator, error) {
	instanceId := uuid.New()

	meter := otel.Meter("operator-meter",
		metric.WithInstrumentationAttributes(attribute.String("instance_id", instanceId.String())))

	var err error
	publishedMessageCounter, err = meter.Int64Counter("gox_published_messages",
		metric.WithDescription("The number of messages published by this instance."))
	rebalanceEventCounter, err = meter.Int64Counter("gox_rebalance_events",
		metric.WithDescription("The number of rebalance events this instance has processed."))

	conn, err := pgx.Connect(ctx, config.DatabaseUrl)
	if err != nil {
		log.WithError(err).Warn("Failed to acquire database connection")
		return nil, err
	}

	err = migrations.ApplyMigrations(config.DatabaseUrl)
	if err != nil {
		log.WithError(err).Warn("Database migration failed")
		return nil, err
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": config.BrokerUrls,
		"client.id":         instanceId,
		"acks":              "all",
	})
	if err != nil {
		log.WithError(err).Warn("Failed to create producer")
		return nil, err
	}

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": config.BrokerUrls,
		"client.id":         instanceId,
		"group.id":          "gox-partitioning-group",
	})
	if err != nil {
		log.WithError(err).Warn("Failed to create consumer")
		return nil, err
	}

	err = consumer.SubscribeTopics(config.Topics, rebalanceCallback)
	if err != nil {
		log.WithError(err).Warn("Failed to subscribe to topics")
		return nil, err
	}

	log.WithField("instanceId", instanceId).Info("Operator initialised")

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
		log.WithError(err).Warn("Failed to get assigned topic partitions")
		return err
	}

	// Pause our consumer against the partitions it has been assigned
	err = o.consumer.Pause(assignedTopicPartitions)
	if err != nil {
		log.WithError(err).Warn("Failed to pause consumer")
		return err
	}

	// If we haven't been assigned any partitions we can return early
	if len(assignedTopicPartitions) == 0 {
		return nil
	}

	// Flatten our assigned partitions into a map of topic-> partitions[]
	topicPartitions := make(map[string][]int, len(assignedTopicPartitions))
	for _, topicPartition := range assignedTopicPartitions {
		topicPartitions[*topicPartition.Topic] = append(topicPartitions[*topicPartition.Topic],
			int(topicPartition.Partition))
	}

	// Start our transaction
	tx, err := o.conn.Begin(ctx)
	if err != nil {
		log.WithError(err).Warn("Failed to start transaction")
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
		log.WithError(err).Warn("Failed to query for pending events")
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
			log.WithError(err).Warn("Failed to construct event")
			return err
		}
		// Create a message from the event
		message := constructMessage(*event)

		// Publish our message to the channel
		err = o.producer.Produce(message, deliveryChan)
		if err != nil {
			log.WithError(err).Warn("Failed to publish event")
			return err
		}

		e := <-deliveryChan
		m := e.(*kafka.Message)

		if m.TopicPartition.Error != nil {
			log.WithError(m.TopicPartition.Error).WithFields(log.Fields{
				"key":       string(m.Key),
				"topic":     *m.TopicPartition.Topic,
				"partition": m.TopicPartition.Partition,
			}).Warn("Delivery failed")
			return err
		} else {
			// Increment our published message counter
			publishedMessageCounter.Add(ctx, 1)

			log.WithFields(log.Fields{
				"key":       string(m.Key),
				"topic":     *m.TopicPartition.Topic,
				"partition": m.TopicPartition.Partition,
			}).Debug("Published message")
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
		log.WithError(err).Warn("Failed to update event status")
		return err
	}

	// Commit the transaction
	err = tx.Commit(ctx)
	if err != nil {
		log.WithError(err).Warn("Failed to commit transaction")
		return err
	}

	log.Infof("Published %d event(s)", len(eventIds))

	return nil
}

func (o *operator) Close(ctx context.Context) {
	log.Info("Closing connections...")
	o.producer.Close()
	o.consumer.Close()
	o.conn.Close(ctx)
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
	// Increment our rebalance event counter
	rebalanceEventCounter.Add(context.Background(), 1)
	switch ev := event.(type) {
	case kafka.AssignedPartitions:
		log.WithField(
			"protocol", c.GetRebalanceProtocol()).Infof(
			"Rebalance: %d new partition(s) assigned", len(ev.Partitions))
	case kafka.RevokedPartitions:
		log.WithField(
			"protocol", c.GetRebalanceProtocol()).Infof(
			"Rebalance: %d partition(s) revoked", len(ev.Partitions))

		if c.AssignmentLost() {
			log.Warn("Current assignment lost")
		}
	}

	return nil
}
