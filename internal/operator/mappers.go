package operator

import (
	"github.com/angusgyoung/gox/pkg"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	log "github.com/sirupsen/logrus"
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
		&event.Headers,
	)

	return &event, err
}

func mapEventToMessage(event pkg.Event) (*kafka.Message, error) {
	headers, err := mapHeaders(event.Headers)
	if err != nil {
		log.WithError(err).Warn("Failed to map headers")
		return nil, err
	}

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &event.Topic,
			Partition: event.Partition,
		},
		Key:       []byte(event.Key),
		Value:     event.Message,
		Headers:   headers,
		Timestamp: event.CreatedTimestamp,
	}

	return message, nil
}

func mapHeaders(headers []pkg.Header) ([]kafka.Header, error) {
	kafkaHeaders := make([]kafka.Header, len(headers))

	for i, v := range headers {

		value, err := v.Value.MarshalJSON()
		if err != nil {
			log.WithError(err).Warn("Failed to marshal header value")
			return nil, err
		}

		kafkaHeaders[i] = kafka.Header{
			Key:   v.Key,
			Value: value,
		}
	}

	return kafkaHeaders, nil
}
