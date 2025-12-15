#!/bin/bash
# Each service owns its own database: no service can read or write another's tables, so the only
# coupling between them is the event contract on Kafka. One Postgres instance keeps local
# development cheap while preserving that boundary.
set -e

for db in order_service inventory_service payment_service shipping_service; do
  echo "Creating database $db"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-EOSQL
    CREATE DATABASE $db;
EOSQL
done
