alter table outbox
drop constraint outbox_pkey,
add constraint unique (id);
