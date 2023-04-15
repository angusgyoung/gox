#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE USER outbox WITH ENCRYPTED PASSWORD 'outbox';
	CREATE DATABASE outbox;
	\c outbox
	CREATE TABLE IF NOT EXISTS outbox (
		id varchar(36) unique not null,
		timestamp timestamp ,
		status varchar(32) not null,
		topic varchar(128) not null,
		key varchar(36) not null,
		message bytea not null
	);
	GRANT ALL PRIVILEGES ON DATABASE outbox TO outbox;
	GRANT ALL ON outbox TO outbox;
EOSQL