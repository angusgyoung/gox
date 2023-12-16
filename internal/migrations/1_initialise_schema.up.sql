CREATE TABLE IF NOT EXISTS outbox
(
    id                varchar(36) UNIQUE NOT NULL DEFAULT gen_random_uuid(),
    created_timestamp timestamp WITHOUT TIME ZONE DEFAULT (now() at time zone 'utc'),
    updated_timestamp timestamp,
    status            varchar(8)         NOT NULL DEFAULT 'PENDING',
    topic             varchar(128)       NOT NULL,
    partition         smallint           NOT NULL,
    key               varchar(64)        NOT NULL,
    message           bytea              NOT NULL,
    instance_id       varchar(36)
);