//go:build integration

package operator

import (
	"context"
	"errors"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// trackingProducer records every message it delivers. Because Execute
// reads from deliveryChan synchronously in the same goroutine, the
// messages slice needs no mutex.
type trackingProducer struct {
	messages []*kafka.Message
}

func (p *trackingProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	p.messages = append(p.messages, msg)
	deliveryChan <- msg
	return nil
}

func (p *trackingProducer) Close() {}

func statusOf(t *testing.T, ctx context.Context, conn *pgx.Conn, id uuid.UUID) string {
	t.Helper()
	var status string
	require.NoError(t, conn.QueryRow(ctx, `SELECT status FROM outbox WHERE id = $1`, id).Scan(&status))
	return status
}

func rowExists(t *testing.T, ctx context.Context, conn *pgx.Conn, id uuid.UUID) bool {
	t.Helper()
	var count int
	require.NoError(t, conn.QueryRow(ctx, `SELECT COUNT(*) FROM outbox WHERE id = $1`, id).Scan(&count))
	return count == 1
}

// TestExecute_UpdateMode verifies that Execute publishes all PENDING events
// and transitions them to SENT within a single transaction.
func TestExecute_UpdateMode(t *testing.T) {
	ctx := context.Background()
	conn := newTestDB(t)

	topic := "test-topic"
	id1 := insertPendingEvent(t, ctx, conn, topic, 0)
	id2 := insertPendingEvent(t, ctx, conn, topic, 0)
	id3 := insertPendingEvent(t, ctx, conn, topic, 0)

	p := &trackingProducer{}
	op := newOperator(conn, p,
		&mockConsumer{partitions: []kafka.TopicPartition{{Topic: &topic, Partition: 0}}},
		&Config{CompletionMode: UpdateCompletionMode, BatchSize: 10},
	)

	require.NoError(t, op.Execute(ctx))

	assert.Len(t, p.messages, 3)
	for _, id := range []uuid.UUID{id1, id2, id3} {
		assert.Equal(t, "SENT", statusOf(t, ctx, conn, id))
	}
}

// TestExecute_DeleteMode verifies that Execute publishes events and then
// removes them from the outbox rather than updating their status.
func TestExecute_DeleteMode(t *testing.T) {
	ctx := context.Background()
	conn := newTestDB(t)

	topic := "test-topic"
	id1 := insertPendingEvent(t, ctx, conn, topic, 0)
	id2 := insertPendingEvent(t, ctx, conn, topic, 0)

	p := &trackingProducer{}
	op := newOperator(conn, p,
		&mockConsumer{partitions: []kafka.TopicPartition{{Topic: &topic, Partition: 0}}},
		&Config{CompletionMode: DeleteCompletionMode, BatchSize: 10},
	)

	require.NoError(t, op.Execute(ctx))

	assert.Len(t, p.messages, 2)
	assert.False(t, rowExists(t, ctx, conn, id1))
	assert.False(t, rowExists(t, ctx, conn, id2))
}

// TestExecute_NoPartitionsAssigned verifies that Execute returns early without
// touching the DB when the consumer has not been assigned any partitions.
func TestExecute_NoPartitionsAssigned(t *testing.T) {
	ctx := context.Background()
	conn := newTestDB(t)

	topic := "test-topic"
	id := insertPendingEvent(t, ctx, conn, topic, 0)

	p := &trackingProducer{}
	op := newOperator(conn, p,
		&mockConsumer{},
		&Config{CompletionMode: UpdateCompletionMode, BatchSize: 10},
	)

	require.NoError(t, op.Execute(ctx))

	assert.Empty(t, p.messages)
	assert.Equal(t, "PENDING", statusOf(t, ctx, conn, id))
}

// TestExecute_NoPendingEvents verifies that Execute skips the completion step
// entirely when there are no events to publish.
func TestExecute_NoPendingEvents(t *testing.T) {
	ctx := context.Background()
	conn := newTestDB(t)

	topic := "test-topic"
	p := &trackingProducer{}
	op := newOperator(conn, p,
		&mockConsumer{partitions: []kafka.TopicPartition{{Topic: &topic, Partition: 0}}},
		&Config{CompletionMode: UpdateCompletionMode, BatchSize: 10},
	)

	require.NoError(t, op.Execute(ctx))
	assert.Empty(t, p.messages)
}

// TestExecute_RespectsBatchSize verifies that Execute publishes at most
// BatchSize events per cycle, leaving the remainder PENDING.
func TestExecute_RespectsBatchSize(t *testing.T) {
	ctx := context.Background()
	conn := newTestDB(t)

	topic := "test-topic"
	for range 10 {
		insertPendingEvent(t, ctx, conn, topic, 0)
	}

	p := &trackingProducer{}
	op := newOperator(conn, p,
		&mockConsumer{partitions: []kafka.TopicPartition{{Topic: &topic, Partition: 0}}},
		&Config{CompletionMode: UpdateCompletionMode, BatchSize: 4},
	)

	require.NoError(t, op.Execute(ctx))

	assert.Len(t, p.messages, 4)

	var sentCount int
	require.NoError(t, conn.QueryRow(ctx, `SELECT COUNT(*) FROM outbox WHERE status = 'SENT'`).Scan(&sentCount))
	assert.Equal(t, 4, sentCount)

	var pendingCount int
	require.NoError(t, conn.QueryRow(ctx, `SELECT COUNT(*) FROM outbox WHERE status = 'PENDING'`).Scan(&pendingCount))
	assert.Equal(t, 6, pendingCount)
}

// TestExecute_RespectsPartitionAssignment verifies that Execute only
// processes events whose partition matches the consumer's assignment.
func TestExecute_RespectsPartitionAssignment(t *testing.T) {
	ctx := context.Background()
	conn := newTestDB(t)

	topic := "test-topic"
	id0 := insertPendingEvent(t, ctx, conn, topic, 0)
	id1 := insertPendingEvent(t, ctx, conn, topic, 1)
	id2 := insertPendingEvent(t, ctx, conn, topic, 2)

	p := &trackingProducer{}
	op := newOperator(conn, p,
		&mockConsumer{partitions: []kafka.TopicPartition{{Topic: &topic, Partition: 1}}},
		&Config{CompletionMode: UpdateCompletionMode, BatchSize: 10},
	)

	require.NoError(t, op.Execute(ctx))

	assert.Len(t, p.messages, 1)
	assert.Equal(t, "PENDING", statusOf(t, ctx, conn, id0))
	assert.Equal(t, "SENT", statusOf(t, ctx, conn, id1))
	assert.Equal(t, "PENDING", statusOf(t, ctx, conn, id2))
}

// TestExecute_KafkaFailure_RollsBack verifies that a delivery failure causes
// the transaction to roll back, leaving all events PENDING.
func TestExecute_KafkaFailure_RollsBack(t *testing.T) {
	ctx := context.Background()
	conn := newTestDB(t)

	topic := "test-topic"
	id := insertPendingEvent(t, ctx, conn, topic, 0)

	deliveryErr := errors.New("broker unavailable")
	op := newOperator(conn,
		&mockProducer{deliveryError: deliveryErr},
		&mockConsumer{partitions: []kafka.TopicPartition{{Topic: &topic, Partition: 0}}},
		&Config{CompletionMode: UpdateCompletionMode, BatchSize: 10},
	)

	err := op.Execute(ctx)
	assert.Equal(t, deliveryErr, err)
	assert.Equal(t, "PENDING", statusOf(t, ctx, conn, id))
}

// TestExecute_MultipleTopics verifies that events across multiple assigned
// topics and partitions are all published in a single Execute cycle.
func TestExecute_MultipleTopics(t *testing.T) {
	ctx := context.Background()
	conn := newTestDB(t)

	topicA := "topic-a"
	topicB := "topic-b"
	idA := insertPendingEvent(t, ctx, conn, topicA, 0)
	idB := insertPendingEvent(t, ctx, conn, topicB, 2)
	unassigned := insertPendingEvent(t, ctx, conn, topicB, 3) // partition not assigned

	p := &trackingProducer{}
	op := newOperator(conn, p,
		&mockConsumer{partitions: []kafka.TopicPartition{
			{Topic: &topicA, Partition: 0},
			{Topic: &topicB, Partition: 2},
		}},
		&Config{CompletionMode: UpdateCompletionMode, BatchSize: 10},
	)

	require.NoError(t, op.Execute(ctx))

	assert.Len(t, p.messages, 2)
	assert.Equal(t, "SENT", statusOf(t, ctx, conn, idA))
	assert.Equal(t, "SENT", statusOf(t, ctx, conn, idB))
	assert.Equal(t, "PENDING", statusOf(t, ctx, conn, unassigned))
}
