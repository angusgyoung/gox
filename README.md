# Gox
![Build](https://github.com/angusgyoung/gox/actions/workflows/build.yml/badge.svg?branch=main)
[![Go Reference](https://pkg.go.dev/badge/github.com/angusgyoung/gox.svg)](https://pkg.go.dev/github.com/angusgyoung/gox)


A small outbox publisher for Postgres/Kafka, with support for kafka-backed parallelisation and rebalancing. Intended to work nicely as a sidecar to a container performing transactional publication.

Inspired by [a fantastic blog post by Alexander Morley and Richard Noble](https://medium.com/babylon-technology-blog/distributed-outbox-event-publishing-pattern-with-kafka-and-sidecars-c57350c0ff7c).


Supports

- postgres >= 11.x
- kafka >= 2.1.1

## Features
- Batched delivery
- Automatic rebalancing and leader election
across instances using Kafka consumer groups
- Multi-topic support
- Structured logging

## Getting Started

### Running Gox

```shell
docker run ghcr.io/angusgyoung/gox:latest --brokers=broker:9092 --topics=client-events-v1 --db=postgres://outbox:outbox@postgres:5432/outbox
```

This will start gox, which will begin to poll the broker. Once the broker has assigned it some partitions, the instance will begin publishing events from the outbox table. 

```
time="2023-05-14T20:46:45Z" level=info msg="Starting gox..."
time="2023-05-14T20:46:45Z" level=info msg="Operator initialised" instanceId=6f1008e8-a5c7-40c2-a0a3-9a64c10ff65e
time="2023-05-14T20:46:45Z" level=info msg="Polling for events..."
```
As instances of gox enter and exit the consumer group, responsibility for each partition of the configured topics will be redistributed amoung the remaining instances. 

### Writing Events

To publish an event, insert a new row into the outbox table. The producing application is responsible for serialising the message content.

Applications should populate the `topic`, `partition`, `key` and `message` columns on inserts:

```sql
insert into outbox ('topic', 'partition', 'key', 'message')
values ( 'client-events-v1', '5', 'message-key', 'bytes');
```

Once the event is written and committed, an instance of gox assigned to the event's partition will publish the
message to the topic, and update its status from `PENDING` to `SENT`, setting `updated_timestamp`
to the timestamp of batch completion.

```sql
insert into outbox (topic, partition, key, message) values ('test-topic-a', generate_series(0, 9), gen_random_uuid(), 'abc');
```

Setting the log level to `debug` will log the details of each event as it is delivered, as well as the number of events published during the interval.

### Database Setup

By default gox will attempt to create the following table on startup, and as such the database should be configured to allow gox to create tables:

```sql
CREATE TABLE IF NOT EXISTS outbox (
		id varchar(36) UNIQUE NOT NULL DEFAULT gen_random_uuid(),
		created_timestamp timestamp WITHOUT TIME ZONE DEFAULT (now() at time zone 'utc'),
		updated_timestamp timestamp,
		status varchar(8) NOT NULL DEFAULT 'PENDING',
		topic varchar(128) NOT NULL,
		partition smallint NOT NULL,
		key varchar(64) NOT NULL,
		message bytea NOT NULL,
		instance_id varchar(36)
);
```

## Configuration

### Database connection
`--db`/`GOX_DB_URL`

Database connection URL. 

### Broker connection
`--brokers`/`GOX_BROKER_URLS`

Comma-separated list of brokers to connect to. 

### Topics
`--topics`/`GOX_TOPICS`

Comma-separated list of topics to subscribe to. Topics present in the outbox table that are not part of this list will **not** be published by gox.

### Poll Interval (optional)
`--pollInterval`/`GOX_POLL_INTERVAL`

Sets the interval between consumer polls in milliseconds. Defaults to `100` (ms).

### Batch Size (optional)
`--batchSize`/`GOX_BATCH_SIZE`

Sets the maximum number of events to be published on each poll. Defaults to `50`.

### Log Level (optional)
`--logLevel`/`GOX_LOG_LEVEL`

Sets the log level. Defaults to `warn`.

### Log Format (optional)
`--logFormat`/`GOX_LOG_FORMAT`

Sets the log format. Available options are `json` and `text`. Defaults to `text`.