package operator

import (
	"context"
	"fmt"
	"github.com/angusgyoung/gox/pkg"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
	"strings"
	"time"
)

const updateEventStatusSql = `
	UPDATE outbox 
	SET status = $1, updated_timestamp = $2, instance_id = $3
	WHERE id = ANY ($4)
`

const selectLatestPendingEventsSql = `
	SELECT * FROM outbox 
	WHERE status = $1
	AND (%s)
	ORDER BY created_timestamp ASC
	LIMIT $2
`

// Fetch any events that are in a PENDING state,
// and are destined for one of the topics we have been assigned
func fetchPendingEvents(ctx context.Context,
	tx pgx.Tx,
	assignedTopicPartitions []kafka.TopicPartition,
	batchSize int) (pgx.Rows, error) {

	// Flatten our assigned partitions into a map of topic-> partitions[]
	topicPartitions := make(map[string][]int, len(assignedTopicPartitions))
	for _, topicPartition := range assignedTopicPartitions {
		topicPartitions[*topicPartition.Topic] = append(topicPartitions[*topicPartition.Topic],
			int(topicPartition.Partition))
	}

	return tx.Query(
		ctx,
		buildFetchPendingEventsQuery(topicPartitions),
		pkg.PENDING,
		batchSize,
	)
}

// Update events in the supplied list to a
// SENT status, and set the updated timestamp to the
// current time, and the instance id to our id.
func updateEventStatus(ctx context.Context,
	instanceId uuid.UUID,
	tx pgx.Tx,
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

func buildFetchPendingEventsQuery(topicPartitions map[string][]int) string {
	conditions := make([]string, len(topicPartitions))
	i := 0
	for topic, partitions := range topicPartitions {
		partitionsStr := formatIntList(partitions)
		conditions[i] = fmt.Sprintf("(topic = '%s' AND partition IN (%s))", topic, partitionsStr)
		i++
	}

	return fmt.Sprintf(selectLatestPendingEventsSql, strings.Join(conditions, " OR "))
}

func formatIntList(integers []int) string {
	strs := make([]string, len(integers))

	for i, n := range integers {
		strs[i] = fmt.Sprintf("%d", n)
	}

	return strings.Join(strs, ",")
}
