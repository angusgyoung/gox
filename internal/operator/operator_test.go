package operator

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	_ = initialiseMeters(uuid.New())
	os.Exit(m.Run())
}

// mockRows controls how many rows are returned. It embeds pgx.Rows so that it
// satisfies the interface without implementing unused methods.
type mockRows struct {
	pgx.Rows
	count     int
	index     int
	scanError error
}

func (r *mockRows) Next() bool {
	if r.index < r.count {
		r.index++
		return true
	}
	return false
}

func (r *mockRows) Scan(dest ...any) error { return r.scanError }
func (r *mockRows) Close()                 {}

// mockTx records Exec calls so tests can assert on completion-mode behaviour.
// It embeds pgx.Tx so that it satisfies the transaction interface without
// implementing unused methods.
type mockTx struct {
	pgx.Tx
	rows        pgx.Rows
	execCount   int
	queryError  error
	execError   error
	commitError error
}

func (t *mockTx) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
	return t.rows, t.queryError
}

func (t *mockTx) Exec(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	t.execCount++
	return pgconn.NewCommandTag(""), t.execError
}

func (t *mockTx) Commit(_ context.Context) error   { return t.commitError }
func (t *mockTx) Rollback(_ context.Context) error { return nil }

type mockConn struct {
	mtx        pgx.Tx
	beginError error
}

func (c *mockConn) Begin(_ context.Context) (pgx.Tx, error) {
	return c.mtx, c.beginError
}

func (c *mockConn) Close(_ context.Context) error { return nil }

// mockProducer synchronously delivers the message back on deliveryChan.
// Set produceError to simulate a synchronous Produce failure.
// Set deliveryError to simulate a failed delivery report.
type mockProducer struct {
	produceError  error
	deliveryError error
}

func (p *mockProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	if p.produceError != nil {
		return p.produceError
	}
	if p.deliveryError != nil {
		msg.TopicPartition.Error = p.deliveryError
	}
	deliveryChan <- msg
	return nil
}

func (p *mockProducer) Close() {}

type mockConsumer struct {
	partitions      []kafka.TopicPartition
	assignmentError error
	pauseError      error
}

func (c *mockConsumer) Poll(_ int) kafka.Event { return nil }

func (c *mockConsumer) SubscribeTopics(_ []string, _ kafka.RebalanceCb) error { return nil }

func (c *mockConsumer) Assignment() ([]kafka.TopicPartition, error) {
	return c.partitions, c.assignmentError
}

func (c *mockConsumer) Pause(_ []kafka.TopicPartition) error { return c.pauseError }
func (c *mockConsumer) Close() error                         { return nil }

// newOperator is a helper to build a test Operator without calling NewOperator.
func newOperator(conn conn, producer producer, consumer consumer, config *Config) *Operator {
	return &Operator{
		instanceId: uuid.New(),
		conn:       conn,
		producer:   producer,
		consumer:   consumer,
		config:     config,
	}
}

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
