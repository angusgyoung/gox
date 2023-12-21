package operator

import (
	"context"
	"testing"

	"github.com/angusgyoung/gox/pkg"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockRow struct {
	mock.Mock
}

func (mr *mockRow) Scan(dest ...any) error {
	return nil
}

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
		&OperatorConfig{},
	}

	err := operator.Execute(context.Background())

	assert.Nil(t, err)
}

func TestConstructEvent(t *testing.T) {
	row := new(mockRow)

	event, err := constructEvent(row)

	assert.Nil(t, err)
	assert.NotNil(t, event)
}

func TestConstructMessage(t *testing.T) {
	event := &pkg.Event{
		Topic:     "topic",
		Partition: 5,
		Key:       "key",
		Message:   []byte("message"),
	}

	message := constructMessage(*event)

	assert.NotNil(t, message)
	assert.Equal(t, event.Topic, *message.TopicPartition.Topic)
	assert.Equal(t, event.Partition, message.TopicPartition.Partition)
	assert.Equal(t, event.Key, string(message.Key))
	assert.Equal(t, event.Message, message.Value)
}
