# Benchmark

Every number here was produced by `loadtest/SagaLoadTest.java` on the hardware described below.
Nothing is extrapolated.

## Method

The harness runs in two phases, which matters for reading the results.

**Submit** measures `POST /orders` alone. This is the only latency a caller ever waits on: the
request returns once the order row and its outbox row commit, before any downstream service has
seen it.

**Settle** measures the time from acceptance until the order reaches a terminal state — five
services and six topics later. Outstanding orders are swept once per round rather than awaited one
at a time, so the observation error is bounded at roughly one sweep (~50–150 ms) per order rather
than accumulating behind a queue of already-finished orders.

Combining the phases would produce a single misleading number: threads would spend most of their
time polling, and "throughput" would describe how fast the saga settles rather than how many orders
the service can accept.

## Environment

| | |
|---|---|
| Host | Apple Silicon laptop, macOS |
| Container runtime | Colima, 4 CPUs / 8 GB |
| In containers | Kafka 7.6 (single broker), Zookeeper, PostgreSQL 16 |
| On the host | 6 Spring Boot services, one JVM each |
| Topics | 6 partitions, replication factor 1 |

Everything shares one machine, so the broker, four databases, six JVMs, and the load generator all
compete for the same four cores. Absolute throughput reflects that; the comparisons between
configurations are what the numbers are for.

## Results

Two workloads. **Unloaded** (100 orders, 4 concurrent) shows what one order costs when nothing is
queued. **Saturated** (2000 orders, 64 concurrent) pushes past capacity so the queue builds.

### Saturated — 2000 orders at concurrency 64

| Configuration | Accepted /s | Submit p99 | Fulfilled /s | Settle p50 |
|---|---:|---:|---:|---:|
| Baseline | 315 | 488 ms | 15.8 | 58.7 s |
| Uniform `concurrency=6` | — | 1580 ms | 18.8 | 64.4 s |
| **Tuned** | **941** | **188 ms** | **42.6** | **26.6 s** |

### Unloaded — 100 orders at concurrency 4

| Configuration | Submit p50 | Submit p99 | Settle p50 | Settle p99 |
|---|---:|---:|---:|---:|
| Baseline | 24.5 ms | 199.4 ms | 5173 ms | 6563 ms |
| **Tuned** | **13.8 ms** | **54.2 ms** | **989 ms** | **1473 ms** |

All runs completed with zero submit failures and every order reaching `SHIPPED`.

### What changed

| | Baseline | Tuned |
|---|---|---|
| Listener concurrency | 1 (default) | 3 on the fulfillment path, 1 on log-only consumers |
| Outbox poll interval | 200 ms | 50 ms |
| Outbox batch | 200 rows | 500 rows |
| Producer `linger.ms` | 0 | 5 |
| Hikari pool | 16 | 32 |

Net: **2.7× fulfilled throughput, 3.0× accepted throughput, 61% off submit p99, and 81% off
unloaded end-to-end latency.**

## What the numbers say

**The obvious tuning was the wrong tuning.** Matching listener concurrency to partition count is
the textbook move, and applying it uniformly barely moved fulfilled throughput (15.8 → 18.8/s)
while tripling submit p99 (488 → 1580 ms). Six services multiply that setting by their listener
count — order-service alone consumes five topics — so "six threads per listener" became well over a
hundred consumer threads on a four-core box. They competed with the broker and the databases for
the same cores, and each service's 16-connection pool was smaller than its own thread count, so
handlers queued on connections before they ever reached Postgres.

**Parallelism belongs where the work is.** notification-service and analytics-service only write
log lines; giving them six threads each took CPU from the path customers actually wait on. Dropping
them to one and holding the fulfillment services at three, with a pool sized above the thread
count, is what produced the 2.7×.

**The poll interval is paid four times.** The saga crosses the outbox on every hop, so the relay's
tick shows up four times in end-to-end latency. Cutting it from 200 ms to 50 ms accounts for most
of the drop in unloaded settle time from 5.2 s to 1.0 s.

**Accepting and fulfilling scale separately.** The API accepts 941 orders/s because a submit only
commits two rows locally; fulfillment runs at 42.6/s because it spans four services, four
transactions, and roughly fifteen event deliveries per order. Under saturation the submit path
stays fast (p99 188 ms) while the backlog grows — which is the property the outbox is there to
provide. Callers are never made to wait for the slowest participant.

**Where it is actually bound.** At 42.6 orders/s the platform is doing ~640 event deliveries and
~170 database transactions per second across four services, on four shared cores. The limit is
host CPU, not consumer parallelism — which is why adding threads stopped helping.

## Reproducing

```bash
# 5432 is often taken by a locally installed Postgres.
export POSTGRES_HOST_PORT=5433 POSTGRES_PORT=5433

docker compose up -d
mvn clean package -DskipTests

for svc in order inventory payment shipping notification analytics; do
  java -jar ${svc}-service/target/${svc}-service-1.0.0-SNAPSHOT.jar &
done

java loadtest/SagaLoadTest.java --orders 2000 --concurrency 64 --product SKU-LOADTEST
```

`SKU-LOADTEST` is seeded deep on purpose. Against the ordinary catalogue a run of more than a
hundred orders exhausts stock and the remainder measure the cancellation path instead.

To reproduce the baseline column, override the settings at startup rather than editing the files:

```bash
java -jar order-service/target/order-service-1.0.0-SNAPSHOT.jar \
  --spring.kafka.listener.concurrency=1 \
  --platform.outbox.poll-interval-ms=200 \
  --platform.outbox.batch-size=200 \
  --platform.outbox.linger-ms=0 \
  --spring.datasource.hikari.maximum-pool-size=16
```
