# Event-Driven Order Platform

![CI](https://github.com/evelyn-kk/event-driven-order-platform-demo/actions/workflows/ci.yml/badge.svg)

English · [简体中文](README.zh-CN.md)

An order fulfillment demo: six Spring Boot services coordinate one order lifecycle over Kafka as a
choreographed saga. There are no synchronous calls between services — each one reacts to events and
publishes its own.

The order goes `CREATED → INVENTORY_RESERVED → PAID → SHIPPED`. order-service accepts the request,
inventory-service reserves stock, payment-service charges, shipping-service creates the shipment. If
stock is short or the charge is declined, `order.cancelled` triggers compensation — stock released,
payment refunded if it went through. notification-service and analytics-service listen in but aren't
part of the saga.

I built it to work through the parts of event-driven systems that are easy to get wrong:

- **Transactional outbox** — business state and the outgoing event commit in one transaction, and a
  relay publishes to Kafka afterwards, so the two can't diverge
- **Idempotent consumers** — the business write and an `(eventId, consumerGroup)` marker commit
  together, so at-least-once redelivery applies once
- **Compensation** — reservations are persisted, so a rollback releases the amount actually held
- **Retry and DLQ** — bounded in-place retry, then dead-lettering, so one bad record can't stall a
  partition; failed records can be inspected and replayed over HTTP

## Tech stack

Java 17 · Spring Boot 3.2 · Spring Kafka · Apache Kafka 3.6 · PostgreSQL 16 · Flyway · Micrometer /
Prometheus / Grafana · Testcontainers · Docker Compose · GitHub Actions → GHCR

## Layout

```
platform-events/        shared event records
platform-messaging/     outbox, deduplication, retry/DLQ, metrics — shared by all services
order-service/          8081 — intake, order state machine, saga projection, DLQ admin
inventory-service/      8082 — stock reservation, release, commit
payment-service/        8083 — charge and refund
shipping-service/       8084 — shipment creation
notification-service/   8085 — customer notifications
analytics-service/      8086 — event tailing
loadtest/               dependency-free JDK load harness
docker/                 Postgres init, Prometheus scrape config, Grafana provisioning
```

## Running it

```bash
export POSTGRES_HOST_PORT=5433 POSTGRES_PORT=5433   # avoids a local Postgres on 5432

docker compose up -d          # Kafka, Zookeeper, Postgres, Kafka UI, Prometheus, Grafana
mvn clean package -DskipTests

for svc in order inventory payment shipping notification analytics; do
  java -jar ${svc}-service/target/${svc}-service-1.0.0-SNAPSHOT.jar &
done
```

Topics are created on broker startup, schemas by Flyway on first connect. Kafka UI at
<http://localhost:8080>, Grafana dashboard at <http://localhost:3000>.

```bash
# happy path
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-001","productId":"SKU-1001","quantity":2,"totalAmount":199.00}'

curl -s "http://localhost:8081/orders/<orderId>"

# force a compensation path
curl -X POST "http://localhost:8081/orders?scenario=OUT_OF_STOCK"    ...
curl -X POST "http://localhost:8081/orders?scenario=PAYMENT_FAILED"  ...
```

`mvn clean verify` runs the integration suite against real Postgres and Kafka via Testcontainers:
duplicate delivery applying once, saga rollback releasing stock, late events not resurrecting a
cancelled order, poison-pill dead lettering, and replay.


## Limitations

Payment approval is a threshold check rather than a call to a real provider, and shipment creation
generates a tracking number instead of booking a courier — the transactional and compensating
behaviour around them is real, the external integrations aren't.
