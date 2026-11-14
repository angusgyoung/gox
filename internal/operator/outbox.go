package operator

import (
	"context"
	"time"

	"github.com/angusgyoung/gox/pkg"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

const updateEventStatusSql = `
	UPDATE outbox
	SET status = $1, updated_timestamp = $2, instance_id = $3
	WHERE id = ANY ($4)
`

const deleteEventSql = `
	DELETE FROM outbox
	WHERE id = ANY ($1)
`

const selectPendingEventsSql = `
	SELECT id, created_timestamp, updated_timestamp, status, topic, partition, key, message, instance_id, headers
	FROM outbox
	WHERE status = $1
	AND (topic, partition) IN (SELECT * FROM unnest($3::text[], $4::int4[]))
	ORDER BY created_timestamp ASC
	LIMIT $2
`

func fetchPendingEvents(ctx context.Context,
	tx transaction,
	assignedTopicPartitions []kafka.TopicPartition,
	batchSize int) (rows, error) {

	topics := make([]string, len(assignedTopicPartitions))
	partitions := make([]int32, len(assignedTopicPartitions))
	for i, tp := range assignedTopicPartitions {
		topics[i] = *tp.Topic
		partitions[i] = tp.Partition
	}

	return tx.Query(ctx, selectPendingEventsSql, pkg.PENDING, batchSize, topics, partitions)
}

func updateEventStatus(ctx context.Context,
	instanceId uuid.UUID,
	tx transaction,
	eventIds []uuid.UUID) error {
	_, err := tx.Exec(ctx,
		updateEventStatusSql,
		pkg.SENT.String(),
		time.Now().UTC(),
		instanceId,
		eventIds)
	if err != nil {
		log.WithError(err).Warn("Failed to update event status")
		return err
	}

	return nil
}

func deleteEvents(ctx context.Context,
	tx transaction,
	eventIds []uuid.UUID) error {
	_, err := tx.Exec(ctx,
		deleteEventSql,
		eventIds)
	if err != nil {
		log.WithError(err).Warn("Failed to delete events")
		return err
	}

	return nil
}
