package internal

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

const selectLatestPendingEventsSql = `
	SELECT * FROM outbox 
	WHERE status = $1
	AND partition = ANY ($2)
	AND topic = $3
	ORDER BY timestamp desc
	LIMIT $4
`

const updateEventStatusSql = `
	UPDATE outbox 
	SET status = $1, timestamp = $2, instance_id = $3
	WHERE id = ANY ($4)
`
