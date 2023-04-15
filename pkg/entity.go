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
	ID        uuid.UUID
	Timestamp time.Time
	Status    EventStatus
	Key       string
	Message   []byte
}
