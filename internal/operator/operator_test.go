//go:build !integration

package operator

import (
	"context"
	"errors"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
)

func TestExecute_NoPartitions(t *testing.T) {
	op := newOperator(
		&mockConn{mtx: &mockTx{rows: &mockRows{}}},
		&mockProducer{},
		&mockConsumer{},
		&Config{},
	)

	err := op.Execute(context.Background())

	assert.Nil(t, err)
}

func TestExecute_WithEvents_UpdateMode(t *testing.T) {
	topic := "test-topic"
	mtx := &mockTx{rows: &mockRows{count: 2}}

	op := newOperator(
		&mockConn{mtx: mtx},
		&mockProducer{},
		&mockConsumer{partitions: []kafka.TopicPartition{
			{Topic: &topic, Partition: 0},
		}},
		&Config{CompletionMode: UpdateCompletionMode, BatchSize: 10},
	)

	err := op.Execute(context.Background())

	assert.Nil(t, err)
	assert.Equal(t, 1, mtx.execCount, "updateEventStatus should be called once")
}

func TestExecute_WithEvents_DeleteMode(t *testing.T) {
	topic := "test-topic"
	mtx := &mockTx{rows: &mockRows{count: 2}}

	op := newOperator(
		&mockConn{mtx: mtx},
		&mockProducer{},
		&mockConsumer{partitions: []kafka.TopicPartition{
			{Topic: &topic, Partition: 0},
		}},
		&Config{CompletionMode: DeleteCompletionMode, BatchSize: 10},
	)

	err := op.Execute(context.Background())

	assert.Nil(t, err)
	assert.Equal(t, 1, mtx.execCount, "deleteEvents should be called once")
}

func TestExecute_NoEvents(t *testing.T) {
	topic := "test-topic"
	mtx := &mockTx{rows: &mockRows{count: 0}}

	op := newOperator(
		&mockConn{mtx: mtx},
		&mockProducer{},
		&mockConsumer{partitions: []kafka.TopicPartition{
			{Topic: &topic, Partition: 0},
		}},
		&Config{CompletionMode: UpdateCompletionMode, BatchSize: 10},
	)

	err := op.Execute(context.Background())

	assert.Nil(t, err)
	assert.Equal(t, 0, mtx.execCount, "no DB write should occur when there are no events")
}

func TestExecute_DeliveryFailure(t *testing.T) {
	topic := "test-topic"
	deliveryErr := errors.New("broker unavailable")

	op := newOperator(
		&mockConn{mtx: &mockTx{rows: &mockRows{count: 1}}},
		&mockProducer{deliveryError: deliveryErr},
		&mockConsumer{partitions: []kafka.TopicPartition{
			{Topic: &topic, Partition: 0},
		}},
		&Config{CompletionMode: UpdateCompletionMode, BatchSize: 10},
	)

	err := op.Execute(context.Background())

	assert.Equal(t, deliveryErr, err)
}

func TestExecute_AssignmentError(t *testing.T) {
	assignmentErr := errors.New("assignment failed")

	op := newOperator(
		&mockConn{mtx: &mockTx{rows: &mockRows{}}},
		&mockProducer{},
		&mockConsumer{assignmentError: assignmentErr},
		&Config{},
	)

	err := op.Execute(context.Background())

	assert.Equal(t, assignmentErr, err)
}

func TestExecute_PauseError(t *testing.T) {
	topic := "test-topic"
	pauseErr := errors.New("pause failed")

	op := newOperator(
		&mockConn{mtx: &mockTx{rows: &mockRows{}}},
		&mockProducer{},
		&mockConsumer{
			partitions: []kafka.TopicPartition{{Topic: &topic, Partition: 0}},
			pauseError: pauseErr,
		},
		&Config{},
	)

	err := op.Execute(context.Background())

	assert.Equal(t, pauseErr, err)
}

func TestExecute_BeginError(t *testing.T) {
	topic := "test-topic"
	beginErr := errors.New("begin failed")

	op := newOperator(
		&mockConn{beginError: beginErr},
		&mockProducer{},
		&mockConsumer{partitions: []kafka.TopicPartition{{Topic: &topic, Partition: 0}}},
		&Config{},
	)

	err := op.Execute(context.Background())

	assert.Equal(t, beginErr, err)
}

func TestExecute_QueryError(t *testing.T) {
	topic := "test-topic"
	queryErr := errors.New("query failed")

	op := newOperator(
		&mockConn{mtx: &mockTx{rows: &mockRows{}, queryError: queryErr}},
		&mockProducer{},
		&mockConsumer{partitions: []kafka.TopicPartition{{Topic: &topic, Partition: 0}}},
		&Config{BatchSize: 10},
	)

	err := op.Execute(context.Background())

	assert.Equal(t, queryErr, err)
}

func TestExecute_ScanError(t *testing.T) {
	topic := "test-topic"
	scanErr := errors.New("scan failed")

	op := newOperator(
		&mockConn{mtx: &mockTx{rows: &mockRows{count: 1, scanError: scanErr}}},
		&mockProducer{},
		&mockConsumer{partitions: []kafka.TopicPartition{{Topic: &topic, Partition: 0}}},
		&Config{BatchSize: 10},
	)

	err := op.Execute(context.Background())

	assert.Equal(t, scanErr, err)
}

func TestExecute_ProduceError(t *testing.T) {
	topic := "test-topic"
	produceErr := errors.New("produce failed")

	op := newOperator(
		&mockConn{mtx: &mockTx{rows: &mockRows{count: 1}}},
		&mockProducer{produceError: produceErr},
		&mockConsumer{partitions: []kafka.TopicPartition{{Topic: &topic, Partition: 0}}},
		&Config{BatchSize: 10},
	)

	err := op.Execute(context.Background())

	assert.Equal(t, produceErr, err)
}

func TestExecute_CompletionError(t *testing.T) {
	topic := "test-topic"
	execErr := errors.New("update failed")
	mtx := &mockTx{rows: &mockRows{count: 1}, execError: execErr}

	op := newOperator(
		&mockConn{mtx: mtx},
		&mockProducer{},
		&mockConsumer{partitions: []kafka.TopicPartition{{Topic: &topic, Partition: 0}}},
		&Config{CompletionMode: UpdateCompletionMode, BatchSize: 10},
	)

	err := op.Execute(context.Background())

	assert.Equal(t, execErr, err)
}

func TestExecute_CommitError(t *testing.T) {
	topic := "test-topic"
	commitErr := errors.New("commit failed")
	mtx := &mockTx{rows: &mockRows{count: 1}, commitError: commitErr}

	op := newOperator(
		&mockConn{mtx: mtx},
		&mockProducer{},
		&mockConsumer{partitions: []kafka.TopicPartition{{Topic: &topic, Partition: 0}}},
		&Config{CompletionMode: UpdateCompletionMode, BatchSize: 10},
	)

	err := op.Execute(context.Background())

	assert.Equal(t, commitErr, err)
}
