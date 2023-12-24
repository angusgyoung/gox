alter table outbox
drop constraint outbox_id_key,
add primary key (id);
