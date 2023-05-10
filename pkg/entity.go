package pkg

import (
	"time"

	"github.com/google/uuid"
)

type EventStatus int8

const (
	PENDING EventStatus = iota
	SENT
)

func (es EventStatus) String() string {
	return []string{"PENDING", "SENT"}[es]
}

type Event struct {
	ID         uuid.UUID `db:"id"`
	Timestamp  time.Time `db:"timestamp"`
	Status     string    `db:"status"`
	Topic      string    `db:"topic"`
	Partition  int32     `db:"partition"`
	Key        string    `db:"key"`
	Message    []byte    `db:"message"`
	InstanceID uuid.UUID `db:"instance_id"`
}
