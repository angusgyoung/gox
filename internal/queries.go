package internal

import (
	"fmt"
	"strings"
)

const createTableSql = `
	CREATE TABLE IF NOT EXISTS outbox (
		id varchar(36) UNIQUE NOT NULL,
		timestamp timestamp NOT NULL,
		status varchar(32) NOT NULL,
		topic varchar(128) NOT NULL,
		partition smallint NOT NULL,
		key varchar(36) NOT NULL,
		message bytea NOT NULL,
		instance_id varchar(36)
	);
`

const updateEventStatusSql = `
	UPDATE outbox 
	SET status = $1, timestamp = $2, instance_id = $3
	WHERE id = ANY ($4)
`

const selectLatestPendingEventsSql = `
	SELECT * FROM outbox 
	WHERE status = $1
	AND (%s)
	ORDER BY timestamp ASC
	LIMIT $2
`

func buildFetchPendingEventsQuery(topicPartitions map[string][]int) string {
	conditions := make([]string, len(topicPartitions))
	i := 0
	for topic, partitions := range topicPartitions {
		partitionsStr := formatIntList(partitions)
		conditions[i] = fmt.Sprintf("(topic = '%s' AND partition IN (%s))", topic, partitionsStr)
		i++
	}

	return fmt.Sprintf(selectLatestPendingEventsSql, strings.Join(conditions, " OR "))
}

func formatIntList(integers []int) string {
	strs := make([]string, len(integers))

	for i, n := range integers {
		strs[i] = fmt.Sprintf("%d", n)
	}

	return strings.Join(strs, ",")
}
