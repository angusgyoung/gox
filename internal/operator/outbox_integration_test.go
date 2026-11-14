//go:build integration

package operator

import (
	"context"
	"testing"
	"time"

	"github.com/angusgyoung/gox/internal/migrations"
	"github.com/angusgyoung/gox/pkg"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tc "github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func newTestDB(t *testing.T) *pgx.Conn {
	t.Helper()
	ctx := context.Background()

	pgContainer, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("outbox"),
		tcpostgres.WithUsername("outbox"),
		tcpostgres.WithPassword("outbox"),
		tc.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { pgContainer.Terminate(ctx) })

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	require.NoError(t, migrations.ApplyMigrations(connStr))

	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close(ctx) })

	return conn
}

func insertPendingEvent(t *testing.T, ctx context.Context, conn *pgx.Conn, topic string, partition int) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	err := conn.QueryRow(ctx,
		`INSERT INTO outbox (status, topic, partition, key, message)
		 VALUES ('PENDING', $1, $2, 'key', 'message') RETURNING id`,
		topic, partition,
	).Scan(&id)
	require.NoError(t, err)
	return id
}

func TestFetchPendingEvents(t *testing.T) {
	ctx := context.Background()
	conn := newTestDB(t)

	topic := "test-topic"

	pendingID := insertPendingEvent(t, ctx, conn, topic, 0)
	insertPendingEvent(t, ctx, conn, topic, 1) // different partition, should not be returned

	_, err := conn.Exec(ctx,
		`INSERT INTO outbox (status, topic, partition, key, message)
		 VALUES ('SENT', $1, 0, 'key', 'message')`,
		topic,
	)
	require.NoError(t, err)

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	rows, err := fetchPendingEvents(ctx, tx, []kafka.TopicPartition{
		{Topic: &topic, Partition: 0},
	}, 10)
	require.NoError(t, err)
	defer rows.Close()

	var ids []uuid.UUID
	for rows.Next() {
		event, err := mapRowToEvent(rows)
		require.NoError(t, err)
		ids = append(ids, event.ID)
	}

	require.Len(t, ids, 1)
	assert.Equal(t, pendingID, ids[0])
}

func TestFetchPendingEvents_BatchSize(t *testing.T) {
	ctx := context.Background()
	conn := newTestDB(t)

	topic := "test-topic"
	for range 5 {
		insertPendingEvent(t, ctx, conn, topic, 0)
	}

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	rows, err := fetchPendingEvents(ctx, tx, []kafka.TopicPartition{
		{Topic: &topic, Partition: 0},
	}, 3)
	require.NoError(t, err)
	defer rows.Close()

	var count int
	for rows.Next() {
		count++
	}

	assert.Equal(t, 3, count)
}

func TestUpdateEventStatus(t *testing.T) {
	ctx := context.Background()
	conn := newTestDB(t)

	topic := "test-topic"
	id := insertPendingEvent(t, ctx, conn, topic, 0)

	instanceID := uuid.New()
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)

	err = updateEventStatus(ctx, instanceID, tx, []uuid.UUID{id})
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	var status string
	var gotInstanceID uuid.UUID
	err = conn.QueryRow(ctx,
		`SELECT status, instance_id FROM outbox WHERE id = $1`, id,
	).Scan(&status, &gotInstanceID)
	require.NoError(t, err)

	assert.Equal(t, pkg.SENT.String(), status)
	assert.Equal(t, instanceID, gotInstanceID)
}

func TestDeleteEvents(t *testing.T) {
	ctx := context.Background()
	conn := newTestDB(t)

	topic := "test-topic"
	id1 := insertPendingEvent(t, ctx, conn, topic, 0)
	id2 := insertPendingEvent(t, ctx, conn, topic, 0)
	preserved := insertPendingEvent(t, ctx, conn, topic, 0)

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)

	err = deleteEvents(ctx, tx, []uuid.UUID{id1, id2})
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	var remaining []uuid.UUID
	rows, err := conn.Query(ctx, `SELECT id FROM outbox`)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var id uuid.UUID
		require.NoError(t, rows.Scan(&id))
		remaining = append(remaining, id)
	}

	require.Len(t, remaining, 1)
	assert.Equal(t, preserved, remaining[0])
}
