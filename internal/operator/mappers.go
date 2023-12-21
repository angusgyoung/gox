package operator

import (
	"github.com/angusgyoung/gox/pkg"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func mapRowToEvent(row scannableRow) (*pkg.Event, error) {
	event := pkg.Event{}

	err := row.Scan(
		&event.ID,
		&event.CreatedTimestamp,
		&event.UpdatedTimestamp,
		&event.Status,
		&event.Topic,
		&event.Partition,
		&event.Key,
		&event.Message,
		&event.InstanceID,
	)

	return &event, err
}

func mapEventToMessage(event pkg.Event) *kafka.Message {
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &event.Topic,
			Partition: event.Partition,
		},
		Key:   []byte(event.Key),
		Value: event.Message,
	}

	return message
}
