create table if not exists outbox
(
    id                varchar(36) unique not null default gen_random_uuid(),
    created_timestamp timestamp without time zone default (now() at time zone 'utc'),
    updated_timestamp timestamp,
    status            varchar(8)         not null default 'PENDING',
    topic             varchar(128)       not null,
    partition         smallint           not null,
    key               varchar(64)        not null,
    message           bytea              not null,
    instance_id       varchar(36)
);