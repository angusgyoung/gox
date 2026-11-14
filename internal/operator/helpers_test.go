package operator

import (
	"context"
	"os"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestMain(m *testing.M) {
	_ = initialiseMeters(uuid.New())
	os.Exit(m.Run())
}

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

func newOperator(conn conn, producer producer, consumer consumer, config *Config) *Operator {
	return &Operator{
		instanceId: uuid.New(),
		conn:       conn,
		producer:   producer,
		consumer:   consumer,
		config:     config,
	}
}
