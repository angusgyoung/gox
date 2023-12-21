package operator

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/metric"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
)

var (
	publishedMessageCounter metric.Int64Counter
	rebalanceEventCounter   metric.Int64Counter
)

type operator struct {
	instanceId uuid.UUID
	conn       conn
	producer   producer
	consumer   consumer
	config     *Config
}

func NewOperator(ctx context.Context, config *Config) (Operator, error) {
	// Create an id for our instance
	instanceId := uuid.New()

	err := initialiseMeters(instanceId)
	if err != nil {
		log.WithError(err).Warn("Failed to initialise meters")
		return nil, err
	}

	conn, err := initialiseDatasource(ctx, config.DatabaseUrl)
	if err != nil {
		log.WithError(err).Warn("Failed to initialise datasource")
		return nil, err
	}

	producer, consumer, err := initialiseKafka(instanceId, config.BrokerUrls, config.Topics)
	if err != nil {
		log.WithError(err).Warn("Failed to initialise kafka")
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
	// Refresh topic assignment
	assignedTopicPartitions, err := refreshTopicAssignment(o.consumer, o.config.PollInterval)

	// If we haven't been assigned any partitions we can return early
	if len(assignedTopicPartitions) == 0 {
		return nil
	}

	// Start our transaction
	tx, err := o.conn.Begin(ctx)
	if err != nil {
		log.WithError(err).Warn("Failed to start transaction")
		return err
	}
	defer func(tx pgx.Tx, ctx context.Context) {
		err := tx.Rollback(ctx)
		if err != nil {
			if !errors.Is(err, pgx.ErrTxClosed) {
				log.WithError(err).Warn("Failed to rollback transaction")
			}
		}
	}(tx, ctx)

	// Query for events where the status is 'PENDING', and the topic
	// and partition has been assigned by the consumer group to this instance
	rows, err := fetchPendingEvents(ctx, tx, assignedTopicPartitions, o.config.BatchSize)
	if err != nil {
		log.WithError(err).Warn("Failed to query for pending events")
		return err
	}
	defer rows.Close()

	// Create a slice to hold the id's of all the events we are going
	// to publish
	var eventIds []uuid.UUID
	// Create a channel to produce events into
	deliveryChan := make(chan kafka.Event, o.config.BatchSize)

	for rows.Next() {
		// Create an event from the row
		event, err := mapRowToEvent(rows)
		if err != nil {
			log.WithError(err).Warn("Failed map row")
			return err
		}

		// Create a message from the event
		message, err := mapEventToMessage(*event)
		if err != nil {
			log.WithError(err).Warn("Failed to map event to message")
			return err
		}

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

	// If we didn't find any events to publish we can return early
	if len(eventIds) == 0 {
		return nil
	}

	// Update the state of the events we have published
	err = updateEventStatus(ctx, o.instanceId, tx, eventIds)
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
	o.consumer.Close()
	o.producer.Close()
	o.conn.Close(ctx)
}

func refreshTopicAssignment(consumer consumer, pollInterval int) ([]kafka.TopicPartition, error) {
	// Poll the consumer. We should then receive any assignment
	// updates which we should use to fetch events from the table
	consumer.Poll(pollInterval)

	// Get the partitions assigned to our consumer
	assignedTopicPartitions, err := consumer.Assignment()
	if err != nil {
		log.WithError(err).Warn("Failed to get assigned topic partitions")
		return nil, err
	}

	// Pause our consumer against the partitions it has been assigned
	err = consumer.Pause(assignedTopicPartitions)
	if err != nil {
		log.WithError(err).Warn("Failed to pause consumer")
		return nil, err
	}

	return assignedTopicPartitions, nil
}
