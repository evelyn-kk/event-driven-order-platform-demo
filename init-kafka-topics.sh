#!/bin/bash
set -e

KAFKA_BROKER="kafka:29092"

echo "Creating Kafka topics..."

kafka-topics --bootstrap-server $KAFKA_BROKER --create --if-not-exists \
  --topic order.created --partitions 6 --replication-factor 1

kafka-topics --bootstrap-server $KAFKA_BROKER --create --if-not-exists \
  --topic inventory.deducted --partitions 6 --replication-factor 1

kafka-topics --bootstrap-server $KAFKA_BROKER --create --if-not-exists \
  --topic inventory.insufficient --partitions 6 --replication-factor 1

kafka-topics --bootstrap-server $KAFKA_BROKER --create --if-not-exists \
  --topic payment.completed --partitions 6 --replication-factor 1

kafka-topics --bootstrap-server $KAFKA_BROKER --create --if-not-exists \
  --topic payment.failed --partitions 6 --replication-factor 1

kafka-topics --bootstrap-server $KAFKA_BROKER --create --if-not-exists \
  --topic shipping.created --partitions 6 --replication-factor 1

kafka-topics --bootstrap-server $KAFKA_BROKER --create --if-not-exists \
  --topic notification.send --partitions 6 --replication-factor 1

kafka-topics --bootstrap-server $KAFKA_BROKER --create --if-not-exists \
  --topic order.cancelled --partitions 6 --replication-factor 1

kafka-topics --bootstrap-server $KAFKA_BROKER --create --if-not-exists \
  --topic order.events.dlq --partitions 3 --replication-factor 1

echo "Topics created:"
kafka-topics --bootstrap-server $KAFKA_BROKER --list
