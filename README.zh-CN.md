# 事件驱动订单平台

![CI](https://github.com/evelyn-kk/event-driven-order-platform/actions/workflows/ci.yml/badge.svg)

[English](README.md) · 简体中文

一个订单履约的 demo：六个 Spring Boot 服务通过 Kafka 以编排式（choreography）saga 协作完成一次订单的完整
生命周期。服务之间没有同步调用，每个服务只消费事件、发布自己的事件。

订单状态流转是 `CREATED → INVENTORY_RESERVED → PAID → SHIPPED`。order-service 接单，inventory-service
锁库存，payment-service 扣款，shipping-service 创建发货单。如果库存不足或扣款被拒，`order.cancelled`
触发补偿——释放库存，已扣款的退款。notification-service 和 analytics-service 只消费事件，不参与 saga。

写这个项目是为了把事件驱动里几个容易做错的地方走一遍：

- **事务性发件箱（transactional outbox）**——业务状态和待发事件在同一个事务里提交，再由 relay 投递到
  Kafka，避免两者不一致
- **消费幂等**——业务写入和 `(eventId, consumerGroup)` 标记一起提交，保证 at-least-once 下的重复投递只
  生效一次
- **补偿**——预留记录是持久化的，回滚时释放的是实际占用的数量
- **重试与死信**——先原地有限次重试，失败后进死信队列，避免单条坏消息卡住整个分区；死信可以通过 HTTP
  查看和重放

## 技术栈

Java 17 · Spring Boot 3.2 · Spring Kafka · Apache Kafka 3.6 · PostgreSQL 16 · Flyway · Micrometer /
Prometheus / Grafana · Testcontainers · Docker Compose · GitHub Actions → GHCR

## 模块

```
platform-events/        共享事件定义
platform-messaging/     发件箱、幂等、重试/死信、指标——各服务共用
order-service/          8081 — 接单、订单状态机、saga 投影、死信管理接口
inventory-service/      8082 — 库存预留、释放、扣减
payment-service/        8083 — 扣款与退款
shipping-service/       8084 — 创建发货单
notification-service/   8085 — 用户通知
analytics-service/      8086 — 事件汇总
loadtest/               无依赖的 JDK 压测脚本
docker/                 Postgres 初始化、Prometheus 抓取配置、Grafana 预置面板
```

## 运行

```bash
export POSTGRES_HOST_PORT=5433 POSTGRES_PORT=5433   # 避开本机 5432 上的 Postgres

docker compose up -d          # Kafka、Zookeeper、Postgres、Kafka UI、Prometheus、Grafana
mvn clean package -DskipTests

for svc in order inventory payment shipping notification analytics; do
  java -jar ${svc}-service/target/${svc}-service-1.0.0-SNAPSHOT.jar &
done
```

Topic 在 broker 启动时创建，表结构由 Flyway 在首次连接时建好。Kafka UI 在 <http://localhost:8080>，
Grafana 面板在 <http://localhost:3000>。

```bash
# 正常流程
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-001","productId":"SKU-1001","quantity":2,"totalAmount":199.00}'

curl -s "http://localhost:8081/orders/<orderId>"

# 触发补偿流程
curl -X POST "http://localhost:8081/orders?scenario=OUT_OF_STOCK"    ...
curl -X POST "http://localhost:8081/orders?scenario=PAYMENT_FAILED"  ...
```

`mvn clean verify` 会通过 Testcontainers 起真实的 Postgres 和 Kafka 跑集成测试，覆盖：重复投递只生效一
次、saga 回滚释放库存、迟到事件不会让已取消的订单复活、坏消息进死信、死信重放。


## 说明

支付审批是阈值判断而非真实支付渠道调用，发货只生成运单号而不对接快递——它们周围的事务和补偿逻辑是真
的，对外集成不是。
