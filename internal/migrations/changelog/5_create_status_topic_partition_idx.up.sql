create index if not exists status_topic_partition_idx 
on outbox (status, topic, partition);