alter table outbox
alter column id
    type varchar(36)
    using id::varchar,
alter column instance_id
    type varchar(36)
    using instance_id::varchar;