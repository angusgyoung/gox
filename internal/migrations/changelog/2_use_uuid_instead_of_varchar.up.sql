alter table outbox
alter column id
    type uuid
    using id::uuid,
alter column instance_id
    type uuid
    using instance_id::uuid;