package operator

import (
	"github.com/angusgyoung/gox/pkg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"
)

type mockRow struct {
	mock.Mock
}

func (mr *mockRow) Scan(dest ...any) error {
	return nil
}

func TestConstructEvent(t *testing.T) {
	row := new(mockRow)

	event, err := mapRowToEvent(row)

	assert.Nil(t, err)
	assert.NotNil(t, event)
}

func TestConstructMessage(t *testing.T) {
	event := &pkg.Event{
		Topic:            "topic",
		Partition:        5,
		Key:              "key",
		Message:          []byte("message"),
		CreatedTimestamp: time.Now(),
	}

	message, err := mapEventToMessage(*event)

	assert.NotNil(t, message)
	assert.Nil(t, err)
	assert.Equal(t, event.Topic, *message.TopicPartition.Topic)
	assert.Equal(t, event.Partition, message.TopicPartition.Partition)
	assert.Equal(t, event.Key, string(message.Key))
	assert.Equal(t, event.Message, message.Value)
	assert.Equal(t, event.CreatedTimestamp, message.Timestamp)
}

func TestConstructMessageWithHeader(t *testing.T) {
	event := &pkg.Event{
		Topic:     "topic",
		Partition: 5,
		Key:       "key",
		Message:   []byte("message"),
		Headers: []pkg.Header{
			{Key: "headerKey1", Value: []byte("headerValue1")},
			{Key: "headerKey2", Value: []byte("headerValue2")},
		},
		CreatedTimestamp: time.Now(),
	}

	message, err := mapEventToMessage(*event)

	assert.NotNil(t, message)
	assert.Nil(t, err)
	assert.Equal(t, event.Topic, *message.TopicPartition.Topic)
	assert.Equal(t, event.Partition, message.TopicPartition.Partition)
	assert.Equal(t, event.Key, string(message.Key))
	assert.Equal(t, event.Message, message.Value)
	assert.Equal(t, event.CreatedTimestamp, message.Timestamp)

	assert.Equal(t, event.Headers[0].Key, message.Headers[0].Key)
	assert.Equal(t, []byte("headerValue1"), message.Headers[0].Value)
	assert.Equal(t, event.Headers[1].Key, message.Headers[1].Key)
	assert.Equal(t, []byte("headerValue2"), message.Headers[1].Value)
}
