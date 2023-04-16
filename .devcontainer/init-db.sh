#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE USER outbox WITH ENCRYPTED PASSWORD 'outbox';
  CREATE DATABASE outbox;
  ALTER DATABASE outbox OWNER TO outbox;
  GRANT ALL PRIVILEGES ON DATABASE outbox TO outbox;
  GRANT ALL ON outbox TO outbox;
  GRANT ALL ON SCHEMA public TO outbox;
EOSQL