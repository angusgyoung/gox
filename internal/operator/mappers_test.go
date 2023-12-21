package operator

import (
	"github.com/angusgyoung/gox/pkg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
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
		Topic:     "topic",
		Partition: 5,
		Key:       "key",
		Message:   []byte("message"),
	}

	message := mapEventToMessage(*event)

	assert.NotNil(t, message)
	assert.Equal(t, event.Topic, *message.TopicPartition.Topic)
	assert.Equal(t, event.Partition, message.TopicPartition.Partition)
	assert.Equal(t, event.Key, string(message.Key))
	assert.Equal(t, event.Message, message.Value)
}
