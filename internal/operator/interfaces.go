package operator

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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
