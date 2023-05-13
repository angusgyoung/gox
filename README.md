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
*TBC*

## Requirements
- postgresql >= 11.x
- librdkafka >= 2.1.1

## Configuration
*TBC*