//go:build integration

package migrations_test

import (
	"context"
	"testing"
	"time"

	"github.com/angusgyoung/gox/internal/migrations"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tc "github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func newPostgresContainer(t *testing.T) (connStr string) {
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

	connStr, err = pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	return connStr
}

func TestApplyMigrations(t *testing.T) {
	connStr := newPostgresContainer(t)

	err := migrations.ApplyMigrations(connStr)
	assert.NoError(t, err)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	// Verify the outbox table exists with all expected columns
	var colCount int
	err = conn.QueryRow(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_name = 'outbox'
		  AND column_name IN (
		    'id', 'created_timestamp', 'updated_timestamp', 'status',
		    'topic', 'partition', 'key', 'message', 'instance_id', 'headers'
		  )
	`).Scan(&colCount)
	require.NoError(t, err)
	assert.Equal(t, 10, colCount, "outbox table should have all ten expected columns")

	// Verify the index on (status, topic, partition) exists
	var indexExists bool
	err = conn.QueryRow(ctx, `
		SELECT EXISTS (
		    SELECT FROM pg_indexes
		    WHERE tablename = 'outbox'
		      AND indexname = 'status_topic_partition_idx'
		)
	`).Scan(&indexExists)
	require.NoError(t, err)
	assert.True(t, indexExists, "index on (status, topic, partition) should exist")
}

func TestApplyMigrations_Idempotent(t *testing.T) {
	connStr := newPostgresContainer(t)

	require.NoError(t, migrations.ApplyMigrations(connStr))

	// Second call should not return an error
	err := migrations.ApplyMigrations(connStr)
	assert.NoError(t, err)
}
