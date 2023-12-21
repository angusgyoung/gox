package operator

import (
	"context"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
)

type mockConn struct {
}

func (cc *mockConn) Begin(ctx context.Context) (pgx.Tx, error) {
	return nil, nil
}

func (cc *mockConn) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return pgconn.NewCommandTag(""), nil
}

func (cc *mockConn) Close(ctx context.Context) error {
	return nil
}

type mockProducer struct {
}

func (p *mockProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	return nil
}

func (p *mockProducer) Close() {}

type mockConsumer struct{}

func (c *mockConsumer) Poll(timeoutMs int) (event kafka.Event) {
	return nil
}

func (c *mockConsumer) SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) error {
	return nil
}

func (c *mockConsumer) Assignment() (partitions []kafka.TopicPartition, err error) {
	return nil, nil
}

func (c *mockConsumer) Pause(partitions []kafka.TopicPartition) error {
	return nil
}

func (c *mockConsumer) Close() error {
	return nil
}

func TestExecute(t *testing.T) {

	conn := new(mockConn)
	producer := new(mockProducer)
	consumer := new(mockConsumer)

	operator := &operator{
		uuid.New(),
		conn,
		producer,
		consumer,
		&Config{},
	}

	err := operator.Execute(context.Background())

	assert.Nil(t, err)
}
