package operator

import (
	"context"
	"errors"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/metric"
)

var (
	publishedMessageCounter metric.Int64Counter
	rebalanceEventCounter   metric.Int64Counter
)

type Operator struct {
	instanceId uuid.UUID
	conn       conn
	producer   producer
	consumer   consumer
	config     *Config
}

func NewOperator(ctx context.Context, config *Config) (*Operator, error) {
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

	return &Operator{
		instanceId: instanceId,
		conn:       conn,
		producer:   producer,
		consumer:   consumer,
		config:     config,
	}, nil
}

func (o *Operator) Execute(ctx context.Context) error {
	assignedTopicPartitions, err := refreshTopicAssignment(o.consumer, o.config.PollInterval)
	if err != nil {
		log.WithError(err).Warn("Failed to refresh topic assignment")
		return err
	}

	if len(assignedTopicPartitions) == 0 {
		return nil
	}

	tx, err := o.conn.Begin(ctx)
	if err != nil {
		log.WithError(err).Warn("Failed to start transaction")
		return err
	}
	defer func() {
		rollbackErr := tx.Rollback(ctx)
		if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			log.WithError(rollbackErr).Warn("Failed to rollback transaction")
		}
	}()

	rows, err := fetchPendingEvents(ctx, tx, assignedTopicPartitions, o.config.BatchSize)
	if err != nil {
		log.WithError(err).Warn("Failed to query for pending events")
		return err
	}
	defer rows.Close()

	var eventIds []uuid.UUID
	deliveryChan := make(chan kafka.Event, 1)

	for rows.Next() {
		event, err := mapRowToEvent(rows)
		if err != nil {
			log.WithError(err).Warn("Failed to map row")
			return err
		}

		message, err := mapEventToMessage(event)
		if err != nil {
			log.WithError(err).Warn("Failed to map event to message")
			return err
		}

		if err := o.producer.Produce(message, deliveryChan); err != nil {
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
			return m.TopicPartition.Error
		}

		publishedMessageCounter.Add(ctx, 1)
		log.WithFields(log.Fields{
			"key":       string(m.Key),
			"topic":     *m.TopicPartition.Topic,
			"partition": m.TopicPartition.Partition,
		}).Debug("Published message")

		eventIds = append(eventIds, event.ID)
	}

	if len(eventIds) == 0 {
		return nil
	}

	if o.config.CompletionMode == UpdateCompletionMode {
		if err := updateEventStatus(ctx, o.instanceId, tx, eventIds); err != nil {
			return err
		}
	} else if o.config.CompletionMode == DeleteCompletionMode {
		if err := deleteEvents(ctx, tx, eventIds); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		log.WithError(err).Warn("Failed to commit transaction")
		return err
	}

	log.Infof("Published %d event(s)", len(eventIds))
	return nil
}

func (o *Operator) Close(ctx context.Context) {
	log.Info("Closing connections...")
	o.consumer.Close()
	o.producer.Close()
	o.conn.Close(ctx)
}

func refreshTopicAssignment(consumer consumer, pollInterval int) ([]kafka.TopicPartition, error) {
	consumer.Poll(pollInterval)

	assignedTopicPartitions, err := consumer.Assignment()
	if err != nil {
		log.WithError(err).Warn("Failed to get assigned topic partitions")
		return nil, err
	}

	if err := consumer.Pause(assignedTopicPartitions); err != nil {
		log.WithError(err).Warn("Failed to pause consumer")
		return nil, err
	}

	return assignedTopicPartitions, nil
}
