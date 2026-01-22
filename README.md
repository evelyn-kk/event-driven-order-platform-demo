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
  └── order-service            writes the order, appends order.created to its outbox
        │
        ▼
      inventory-service        reserves stock against a row lock
        ├── enough      → inventory.deducted
        └── not enough  → inventory.insufficient ──────────────┐
                │                                              │
                ▼                                              │
      payment-service          charges the amount it recorded  │
        ├── approved    → payment.completed                    │
        └── declined    → payment.failed ──────────────────────┤
                │                                              │
                ▼                                              ▼
      shipping-service                            order-service cancels the order
        └── shipping.created                        └── order.cancelled
                │                                              │
                ▼                                              ▼
      inventory commits the hold              inventory releases the hold,
      order-service marks it SHIPPED          payment refunds if it had charged

notification-service  fans out customer notifications
analytics-service     tails every topic for downstream analytics
```

The steps are deliberately serial. Charging in parallel with the stock check — the obvious way to
cut latency — means taking money for orders that turn out to be unfulfillable, and leaves the
order's own state machine with no linear order to enforce.

---

## Architecture decisions

| Concern | Approach |
|---|---|
| **Service coupling** | Choreography over Kafka topics — no synchronous inter-service calls |
| **Ordering** | Producers key every record by `orderId`, so one order's events land on one partition and are consumed in order |
| **Isolation** | Each service owns its own consumer group; a slow analytics consumer cannot stall fulfillment |
| **Producer durability** | `acks=all` + `enable.idempotence=true` to survive broker retries without duplicating records |
| **Event contracts** | Shared `platform-events` module of Java records; Spring Kafka's `__TypeId__` header drives typed deserialization |
| **Dual-write consistency** | Business state and the resulting event commit in one transaction via a [transactional outbox](platform-messaging/src/main/java/io/github/evelynkk/orderplatform/messaging/outbox/OutboxRecorder.java); a relay publishes afterwards |
| **Duplicate delivery** | Consumers commit an `(eventId, consumerGroup)` marker alongside their business write, making redelivery a no-op |
| **Compensation** | `order.cancelled` drives every rollback path: inventory releases its hold, payment refunds a completed charge |
| **Contention on hot keys** | Stock reservation takes a `SELECT … FOR UPDATE` row lock rather than retrying an optimistic version, which collapses under a promoted SKU |
| **Service data ownership** | payment-service copies the order amount from `order.created` instead of calling back into order-service, so no service needs another to be up |

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

Integration tests run against real Postgres and Kafka via Testcontainers, so a Docker daemon must
be reachable:

```bash
mvn clean verify
```

On Colima, point Testcontainers at its socket first:

```bash
export DOCKER_HOST="unix://$HOME/.colima/default/docker.sock"
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
```

The build pins the Docker API version it negotiates (`docker.api.version`, default `1.43`) because
docker-java's default of 1.32 is rejected by Docker Engine 25 and newer. Override it with
`-Ddocker.api.version=...` against an older daemon.

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

- [x] PostgreSQL-backed order state machine
- [x] Transactional outbox to close the write-database-then-publish gap
- [x] Consumer-side deduplication for effectively-once processing
- [x] Inventory reservations with a compensating release
- [x] Payment and shipping persistence
- [ ] Tiered retry with exponential backoff, DLQ routing, and a replay endpoint
- [ ] Testcontainers coverage across every service
- [ ] Micrometer → Prometheus → Grafana: consumer lag, end-to-end latency, throughput
- [ ] Load-test harness and a documented tuning baseline

### Known limitations at this commit

Payment approval is a threshold check rather than a call to a provider, and shipment creation
generates a tracking number rather than booking a courier — the surrounding transactional and
compensating behaviour is real, the external integrations are not. notification-service and
analytics-service remain stateless log consumers. The Schema Registry container is provisioned but
not yet wired up; serialization is JSON with a `__TypeId__` header.
