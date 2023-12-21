package pkg

import (
	"database/sql"
	"encoding/json"
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

type Header struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

type Event struct {
	ID               uuid.UUID    `db:"id"`
	CreatedTimestamp time.Time    `db:"created_timestamp"`
	UpdatedTimestamp sql.NullTime `db:"updated_timestamp"`
	Status           string       `db:"status"`
	Topic            string       `db:"topic"`
	Partition        int32        `db:"partition"`
	Key              string       `db:"key"`
	Message          []byte       `db:"message"`
	Headers          []Header     `db:"headers"`
	InstanceID       uuid.UUID    `db:"instance_id"`
}
