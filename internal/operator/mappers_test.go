package operator

import (
	"errors"
	"testing"
	"time"

	"github.com/angusgyoung/gox/pkg"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// populatingRow populates the fields that mapRowToEvent scans into, using
// type assertions so unrecognised positions are silently skipped.
type populatingRow struct {
	id        uuid.UUID
	topic     string
	partition int32
	key       string
	message   []byte
}

func (r *populatingRow) Scan(dest ...any) error {
	// scan order mirrors mapRowToEvent:
	// 0:ID 1:CreatedTimestamp 2:UpdatedTimestamp 3:Status 4:Topic
	// 5:Partition 6:Key 7:Message 8:InstanceID 9:Headers
	if p, ok := dest[0].(*uuid.UUID); ok {
		*p = r.id
	}
	if p, ok := dest[4].(*string); ok {
		*p = r.topic
	}
	if p, ok := dest[5].(*int32); ok {
		*p = r.partition
	}
	if p, ok := dest[6].(*string); ok {
		*p = r.key
	}
	if p, ok := dest[7].(*[]byte); ok {
		*p = r.message
	}
	return nil
}

type errorRow struct{ err error }

func (r *errorRow) Scan(...any) error { return r.err }

func TestConstructEvent_Fields(t *testing.T) {
	id := uuid.New()
	row := &populatingRow{
		id:        id,
		topic:     "test-topic",
		partition: 3,
		key:       "test-key",
		message:   []byte("test-message"),
	}

	event, err := mapRowToEvent(row)

	assert.Nil(t, err)
	assert.Equal(t, id, event.ID)
	assert.Equal(t, "test-topic", event.Topic)
	assert.Equal(t, int32(3), event.Partition)
	assert.Equal(t, "test-key", event.Key)
	assert.Equal(t, []byte("test-message"), event.Message)
}

func TestConstructEvent_ScanError(t *testing.T) {
	scanErr := errors.New("scan error")

	_, err := mapRowToEvent(&errorRow{err: scanErr})

	assert.Equal(t, scanErr, err)
}

func TestConstructMessage(t *testing.T) {
	event := &pkg.Event{
		Topic:            "topic",
		Partition:        5,
		Key:              "key",
		Message:          []byte("message"),
		CreatedTimestamp: time.Now(),
	}

	message, err := mapEventToMessage(event)

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

	message, err := mapEventToMessage(event)

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
