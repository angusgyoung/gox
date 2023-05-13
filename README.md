# Gox
![Build](https://github.com/angusgyoung/gox/actions/workflows/build.yml/badge.svg?branch=main)

A small outbox publisher for Postgres/Kafka, with support for kafka-backed parallelisation and rebalancing. Intended to work nicely as a sidecar to a container performing transactional publication.

Inspired by [a fantastic blog post by Alexander Morley and Richard Noble](https://medium.com/babylon-technology-blog/distributed-outbox-event-publishing-pattern-with-kafka-and-sidecars-c57350c0ff7c).

## Todo
- [x] Batching
- [x] Better configuration
- [x] Proper multi-topic support
- [ ] Better logging
- [ ] Tests
- [ ] Docs ;)
- [ ] 1.0 :tada:

## Usage

```sql
begin

# Other transactional database interactions
...
# Write outbox event
insert into outbox ('topic', 'partition', 'key', 'message')
values ( 'events-v1', '5', 'message-key', 'message-body');

end
```

## Requirements
- postgresql >= 11.x
- kafka >= 2.1.1

## Configuration

### `GOX_DB_URL`

Sets the database connection URL.

### `GOX_BROKER_URLS`

Comma-separated list of brokers to connect to. 

### `GOX_TOPICS`

Comma-separated list of topics to subscribe to. Topics present in the outbox table that are not part of this list will **not** be published by Gox.

### `GOX_POLL_INTERVAL` (optional)

Sets the interval between consumer polls in milliseconds. Defaults to 100 (ms).

### `GOX_BATCH_SIZE` (optional)

Sets the maximum number of events to be published on each poll. Defaults to 50.