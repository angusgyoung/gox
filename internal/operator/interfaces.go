package operator

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type Operator interface {
	Execute(ctx context.Context) error
	Close(ctx context.Context)
}

type Config struct {
	// Interval between consumer polls
	PollInterval int
	// Number of events to attempt to fetch on each
	// interval
	BatchSize int
	// Postgres connection url
	DatabaseUrl string
	// Comma-separated set of bootstrap urls
	BrokerUrls string
	// List of topics to subscribe to
	Topics []string
	// Defines how gox should handle completed events.
	CompletionMode CompletionMode
}

type CompletionMode int8

const (
	UpdateCompletionMode CompletionMode = iota
	DeleteCompletionMode
)

// TODO these are just for mocking stuff I can't construct in a unit test. I think
//  there must be a better way of doing this...

type conn interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Close(ctx context.Context) error
}

type producer interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Close()
}

type consumer interface {
	Poll(timeoutMs int) (event kafka.Event)
	SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) (err error)
	Assignment() (partitions []kafka.TopicPartition, err error)
	Pause(partitions []kafka.TopicPartition) (err error)
	Close() error
}

type scannableRow interface {
	Scan(dest ...any) error
}
