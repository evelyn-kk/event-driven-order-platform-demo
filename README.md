# Event-Driven Order Platform

![CI](https://github.com/evelyn-kk/event-driven-order-platform/actions/workflows/ci.yml/badge.svg)

An e-commerce order fulfillment platform built as a **choreographed saga** over Apache Kafka.
Six Spring Boot services coordinate a single order lifecycle without any direct service-to-service
calls — every state transition is an event.

The focus of this project is the hard parts of running Kafka in production: **dual-write
consistency**, **duplicate delivery**, **partial-failure compensation**, and **poison-pill
containment** — not CRUD.

---

## Order lifecycle

```
POST /orders
  └── order-service publishes order.created
        ├── inventory-service reserves stock
        │     ├── enough    → inventory.deducted
        │     └── not enough → inventory.insufficient ─┐
        ├── payment-service charges the order          │
        │     ├── success → payment.completed          │
        │     └── failure → payment.failed ────────────┤
        │                                              │
        └── shipping-service (on payment.completed) → shipping.created
                                                       │
   order-service consumes the two failure events ──────┘
        └── publishes order.cancelled, driving compensation

notification-service  fans out customer notifications
analytics-service     tails every topic for downstream analytics
```

---

## Architecture decisions

| Concern | Approach |
|---|---|
| **Service coupling** | Choreography over Kafka topics — no synchronous inter-service calls |
| **Ordering** | Producers key every record by `orderId`, so one order's events land on one partition and are consumed in order |
| **Isolation** | Each service owns its own consumer group; a slow analytics consumer cannot stall fulfillment |
| **Producer durability** | `acks=all` + `enable.idempotence=true` to survive broker retries without duplicating records |
| **Event contracts** | Shared `platform-events` module of Java records; Spring Kafka's `__TypeId__` header drives typed deserialization |
| **Compensation** | Failure events (`inventory.insufficient`, `payment.failed`) drive a cancellation path rather than a distributed lock or 2PC |

---

## Tech stack

Java 17 · Spring Boot 3.2 · Spring Kafka · Apache Kafka 3.6 (Confluent 7.6) · Maven multi-module ·
Docker Compose · GitHub Actions → GHCR

---

## Modules

```
event-driven-order-platform/
├── platform-events/        shared event records
├── order-service/          8081 — order intake, cancellation orchestration
├── inventory-service/      8082 — stock reservation and release
├── payment-service/        8083 — charge processing
├── shipping-service/       8084 — shipment creation
├── notification-service/   8085 — customer notifications
├── analytics-service/      8086 — event tailing for analytics
├── docker-compose.yml      Kafka, Zookeeper, Schema Registry, Kafka UI
├── docker-compose.images.yml   run the full stack from prebuilt GHCR images
└── init-kafka-topics.sh    topic provisioning
```

---

## Running locally

Start the infrastructure (Docker Desktop or Colima both work):

```bash
docker compose up -d
```

Topics are provisioned automatically on broker startup (~15–30s). Verify:

```bash
docker exec event-driven-order-platform-kafka \
  kafka-topics --bootstrap-server kafka:29092 --list
```

Build and run the services:

```bash
mvn clean package -DskipTests

java -jar order-service/target/order-service-1.0.0-SNAPSHOT.jar
java -jar inventory-service/target/inventory-service-1.0.0-SNAPSHOT.jar
java -jar payment-service/target/payment-service-1.0.0-SNAPSHOT.jar
java -jar shipping-service/target/shipping-service-1.0.0-SNAPSHOT.jar
java -jar notification-service/target/notification-service-1.0.0-SNAPSHOT.jar
java -jar analytics-service/target/analytics-service-1.0.0-SNAPSHOT.jar
```

Or skip the build and pull the images published by CI:

```bash
export DOCKER_IMAGE_OWNER=evelyn-kk
docker compose -f docker-compose.images.yml up -d
```

Kafka UI (topics, consumer groups, partition offsets): <http://localhost:8080>

---

## Exercising the saga

Happy path:

```bash
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-001","productId":"SKU-1001","quantity":2,"totalAmount":199.00}'
```

Insufficient stock — drives `inventory.insufficient` → `order.cancelled`:

```bash
curl -X POST "http://localhost:8081/orders?scenario=OUT_OF_STOCK" \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-001","productId":"SKU-1001","quantity":2,"totalAmount":199.00}'
```

Payment failure — drives `payment.failed` → `order.cancelled` + stock release:

```bash
curl -X POST "http://localhost:8081/orders?scenario=PAYMENT_FAILED" \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-001","productId":"SKU-1001","quantity":2,"totalAmount":199.00}'
```

---

## Topics

| Topic | Meaning | Partitions |
|---|---|---|
| `order.created` | Order accepted | 6 |
| `inventory.deducted` | Stock reserved | 6 |
| `inventory.insufficient` | Reservation rejected | 6 |
| `payment.completed` | Charge succeeded | 6 |
| `payment.failed` | Charge rejected | 6 |
| `shipping.created` | Shipment created | 6 |
| `order.cancelled` | Saga compensation trigger | 6 |
| `notification.send` | Outbound notifications | 6 |
| `order.events.dlq` | Dead letters | 3 |

Six partitions per topic sets the ceiling on per-service consumer parallelism; `orderId` keying
keeps per-order ordering intact as consumers scale out.

---

## Tests

```bash
mvn clean verify
```

---

## CI/CD

`.github/workflows/ci.yml` runs on every push and pull request:

1. **Build & test** — `mvn clean verify`, uploads the executable jars as artifacts.
2. **Publish** — builds each service image and pushes to GitHub Container Registry
   (skipped on pull requests). Tags: `sha-<short>`, `latest` on `main`, `vX.Y.Z` on tags.

```
ghcr.io/evelyn-kk/event-driven-order-platform-<service>:latest
```

---

## Roadmap

Work in progress, in order:

- [ ] PostgreSQL-backed order state machine and inventory reservations
- [ ] Transactional outbox to close the write-database-then-publish gap
- [ ] Consumer-side deduplication for effectively-once processing
- [ ] Tiered retry with exponential backoff, DLQ routing, and a replay endpoint
- [ ] Testcontainers end-to-end coverage: duplicate delivery, saga rollback, dead lettering
- [ ] Micrometer → Prometheus → Grafana: consumer lag, end-to-end latency, throughput
- [ ] Load-test harness and a documented tuning baseline

### Known limitations at this commit

Business logic in the inventory, payment, shipping, notification, and analytics services is
simulated in memory. Nothing is persisted yet, so compensation currently logs its intent rather
than restoring state. The Schema Registry container is provisioned but not yet wired up —
serialization is JSON. The roadmap above closes each of these.
