package operator

import (
	"context"
	"github.com/angusgyoung/gox/internal/migrations"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

func initialiseMeters(instanceId uuid.UUID) error {
	meter := otel.Meter("operator-meter",
		metric.WithInstrumentationAttributes(attribute.String("instance_id", instanceId.String())))

	var err error
	publishedMessageCounter, err = meter.Int64Counter("gox_published_messages",
		metric.WithDescription("The number of messages published by this instance."))
	rebalanceEventCounter, err = meter.Int64Counter("gox_rebalance_events",
		metric.WithDescription("The number of rebalance events this instance has processed."))

	return err
}

func initialiseDatasource(ctx context.Context, databaseUrl string) (conn, error) {
	conn, err := pgx.Connect(ctx, databaseUrl)
	if err != nil {
		log.WithError(err).Warn("Failed to acquire database connection")
		return nil, err
	}

	err = migrations.ApplyMigrations(databaseUrl)
	if err != nil {
		log.WithError(err).Warn("Database migration failed")
		return nil, err
	}

	return conn, nil
}

func initialiseKafka(instanceId uuid.UUID, brokerUrls string, topics []string) (producer, consumer, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokerUrls,
		"client.id":         instanceId,
		"acks":              "all",
	})
	if err != nil {
		log.WithError(err).Warn("Failed to create producer")
		return nil, nil, err
	}

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokerUrls,
		"client.id":         instanceId,
		"group.id":          "gox-partitioning-group",
	})
	if err != nil {
		log.WithError(err).Warn("Failed to create consumer")
		return nil, nil, err
	}

	err = consumer.SubscribeTopics(topics, rebalanceCallback)
	if err != nil {
		log.WithError(err).Warn("Failed to subscribe to topics")
		return nil, nil, err
	}

	return producer, consumer, nil
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
