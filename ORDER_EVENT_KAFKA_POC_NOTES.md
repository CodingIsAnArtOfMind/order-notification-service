# Order Event POC — Kafka Learning Notes

## 1. Goal of the POC

The purpose of this POC is to learn Kafka and event-driven architecture by starting with a deliberately simple implementation, observing its problems, and improving it step by step.

Current services:

```text
order-event-service
    |
    | POST /api/orders
    | Save order in PostgreSQL
    | Publish OrderPlacedEvent
    v
Kafka
    |
    | topic: order-events
    v
order-notification-service
    |
    | @KafkaListener
    v
Log / Notification
```

The plan is **not** to start with the "perfect" production architecture.

We first build the simple version, create failures intentionally, understand what happens, and then introduce patterns such as retry, DLT/DLQ, idempotency, Outbox, multiple partitions/brokers, OpenSearch, etc.

---

# 2. Current Technology Stack

## order-event-service

- Java 17
- Spring Boot 4.1.1
- Spring Web
- Spring Data JPA
- PostgreSQL
- Spring for Apache Kafka
- Jackson 3
- Lombok

## order-notification-service

- Java 17
- Spring Boot 4.1.1
- Spring for Apache Kafka
- Spring Boot JSON / Jackson 3
- Lombok

## Infrastructure

- PostgreSQL
- Kafka 4.3.1
- Docker CLI
- Colima
- DBeaver

---

# 3. PostgreSQL Setup

Database:

```text
order_db
```

Service-owned schema:

```text
order_event
```

Current table:

```text
order_event.orders
```

Example configuration:

```properties
spring.datasource.url=jdbc:postgresql://localhost:5432/order_db
spring.datasource.username=postgres
spring.datasource.password=YOUR_PASSWORD

spring.jpa.hibernate.ddl-auto=update
spring.jpa.properties.hibernate.default_schema=order_event
spring.jpa.properties.hibernate.hbm2ddl.create_namespaces=true
```

Important distinction:

```text
hibernate.default_schema
```

tells Hibernate which schema to use.

```text
hibernate.hbm2ddl.create_namespaces=true
```

allows Hibernate to create the schema/namespace if it does not already exist.

```text
ddl-auto=update
```

creates/updates tables based on JPA mappings.

Later we should replace automatic schema modification with a migration tool such as Flyway.

---

# 4. Current Producer Flow

The `order-event-service` exposes an HTTP API.

```text
Postman
   |
   | POST /api/orders
   v
OrderController
   |
   v
OrderService
   |
   +----> PostgreSQL
   |
   +----> Kafka
```

Current service behavior:

```java
@Transactional
public OrderResponse createOrder(CreateOrderRequest request) {

    OrderEntity savedOrder = orderRepository.save(order);

    OrderPlacedEvent event = new OrderPlacedEvent(
            savedOrder.getId(),
            savedOrder.getCustomerId(),
            savedOrder.getProductId(),
            savedOrder.getQuantity(),
            savedOrder.getCreatedAt()
    );

    kafkaTemplate.send(
            "order-events",
            savedOrder.getId(),
            event
    );

    // return response
}
```

KafkaTemplate:

```java
private final KafkaTemplate<Long, OrderPlacedEvent> kafkaTemplate;
```

Kafka producer configuration:

```properties
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.LongSerializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JacksonJsonSerializer
```

---

# 5. Kafka Broker — What We Learned

A Kafka **broker** is a Kafka server/node.

A topic does not exist independently from brokers. Topic partitions are stored on brokers.

Mental model:

```text
Producer
   |
   v
Kafka Broker
   |
   +---- topic: order-events
            |
            +---- partition 0
```

Our current environment has one broker.

Topic description showed:

```text
Topic: order-events
PartitionCount: 1
ReplicationFactor: 1

Partition: 0
Leader: 1
Replicas: 1
Isr: 1
```

Meaning:

```text
Kafka Cluster
    |
    +---- Broker 1
            |
            +---- order-events
                    |
                    +---- Partition 0
                            Leader = Broker 1
```

### Important terms

**Leader**

The broker that handles reads/writes for a partition.

**Replica**

A copy of a partition stored on a broker.

**ISR**

In-Sync Replicas.

These are replicas that are sufficiently caught up with the leader.

Current setup:

```text
ReplicationFactor = 1
```

So there is no backup copy.

If Broker 1 dies, the partition becomes unavailable.

---

# 6. Bootstrap Servers and Multi-Broker Routing

Producer configuration:

```properties
spring.kafka.bootstrap-servers=localhost:9092
```

The bootstrap broker is mainly the producer's entry point into the Kafka cluster.

In a multi-broker cluster:

```properties
spring.kafka.bootstrap-servers=broker1:9092,broker2:9092,broker3:9092
```

The producer does not randomly send every message to one of these brokers.

Conceptually:

```text
Producer
   |
   | connect to a bootstrap broker
   v
Kafka cluster
   |
   | metadata
   v
Producer learns:
    partition 0 -> leader broker 2
    partition 1 -> leader broker 3
    partition 2 -> leader broker 1
```

Then:

```text
choose partition
      |
      v
find leader broker for partition
      |
      v
send directly to leader
```

Important mental model:

```text
partition first
broker second
```

We will test this later with multiple brokers and partitions.

---

# 7. Kafka Topic and Partition

Current topic:

```text
order-events
```

Current configuration:

```text
Partitions: 1
Replication Factor: 1
```

Current physical structure:

```text
order-events
    |
    +---- partition 0
```

A Kafka topic is a logical stream/category.

A partition is the actual ordered log inside the topic.

---

# 8. Kafka Key

We publish:

```java
kafkaTemplate.send(
        "order-events",
        savedOrder.getId(),
        event
);
```

Meaning:

```text
Topic = order-events
Key   = orderId
Value = OrderPlacedEvent
```

Example:

```text
Key:
3

Value:
{
  "orderId": 3,
  "customerId": 101,
  "productId": 5001,
  "quantity": 2,
  "placedAt": "..."
}
```

We use:

```java
KafkaTemplate<Long, OrderPlacedEvent>
```

therefore the key serializer is:

```properties
org.apache.kafka.common.serialization.LongSerializer
```

Kafka internally stores:

```text
Key   -> byte[]
Value -> byte[]
```

A producer serializer converts Java objects into bytes.

A consumer deserializer converts those bytes back into Java objects.

For a Long key:

```text
Long 3
   |
   | LongSerializer
   v
byte[]
   |
   v
Kafka
   |
   | LongDeserializer
   v
Long 3
```

---

# 9. Why the Console Consumer Initially Showed a Blank Key

When the console consumer was run without a Long deserializer, it showed:

```text
 -> {"orderId":3,...}
```

The key was not actually missing.

`LongSerializer` stores the Long as binary bytes, not as the text `"3"`.

Using:

```text
key.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```

allowed the console consumer to display:

```text
3 -> {"orderId":3,...}
```

---

# 10. Producer JSON Serialization Issue

We initially got:

```text
ClassNotFoundException:
com.fasterxml.jackson.core.type.TypeReference
```

because Spring Boot 4 uses Jackson 3.

The old serializer:

```properties
org.springframework.kafka.support.serializer.JsonSerializer
```

was not the correct choice for our Boot 4 / Spring Kafka 4 setup.

We switched to:

```properties
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JacksonJsonSerializer
```

Jackson 3 uses packages such as:

```text
tools.jackson.*
```

instead of the older:

```text
com.fasterxml.jackson.*
```

---

# 11. Current Consumer Service

The real Kafka consumer is:

```text
order-notification-service
```

It replaces the temporary Kafka console consumer.

Architecture:

```text
order-event-service
        |
        v
Kafka: order-events
        |
        v
order-notification-service
        |
        v
@KafkaListener
```

Event model:

```java
public record OrderPlacedEvent(
        Long orderId,
        Long customerId,
        Long productId,
        Integer quantity,
        LocalDateTime placedAt
) {
}
```

Current listener:

```java
@KafkaListener(topics = "order-events")
public void handleOrderPlaced(
        ConsumerRecord<Long, OrderPlacedEvent> record) {

    OrderPlacedEvent event = record.value();

    System.out.println(
            "Received event"
                    + " | key=" + record.key()
                    + " | partition=" + record.partition()
                    + " | offset=" + record.offset()
                    + " | orderId=" + event.orderId()
                    + " | customerId=" + event.customerId()
                    + " | productId=" + event.productId()
    );

    if (event.productId().equals(9999L)) {
        System.out.println("Simulating notification failure...");
        throw new RuntimeException("Notification service failed");
    }

    System.out.println(
            "Notification: Order "
                    + event.orderId()
                    + " placed successfully!"
    );
}
```

The `9999` condition is temporary and exists only to simulate a failed consumer.

---

# 12. Consumer Configuration

Current consumer group:

```text
order-notification-group
```

Example configuration:

```properties
spring.application.name=order-notification-service

spring.kafka.bootstrap-servers=localhost:9092

spring.kafka.consumer.group-id=order-notification-group

spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.LongDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JacksonJsonDeserializer

spring.kafka.consumer.properties[spring.json.use.type.headers]=false
spring.kafka.consumer.properties[spring.json.value.default.type]=io.raza.ordernotificationservice.event.OrderPlacedEvent
spring.kafka.consumer.properties[spring.json.trusted.packages]=io.raza.ordernotificationservice.event
```

We set:

```properties
spring.json.use.type.headers=false
```

because producer and consumer each have their own `OrderPlacedEvent` class in different Java packages.

The consumer is explicitly told which local type should be created from the JSON.

---

# 13. Consumer Jackson Dependency Issue

The notification service initially failed with:

```text
NoClassDefFoundError:
tools/jackson/core/type/TypeReference
```

Reason:

The service had Kafka + Lombok, but did not have Jackson 3 available.

Fix:

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-json</artifactId>
</dependency>
```

This gave the notification service the required Jackson 3 classes.

---

# 14. Consumer Group

A `groupId` identifies a **consumer group**.

Current group:

```text
order-notification-group
```

Important rule:

```text
same groupId
    -> consumers share the work

different groupId
    -> each group independently receives the events
```

Example:

```text
                    order-events
                         |
             +-----------+-----------+
             |                       |
             v                       v
order-notification-group     order-analytics-group
             |                       |
             v                       v
     send notification         build analytics
```

If a topic has 3 partitions and we run 3 consumers in the same group:

```text
Consumer 1 -> partition 0
Consumer 2 -> partition 1
Consumer 3 -> partition 2
```

If we run 5 consumers but have only 3 partitions:

```text
Consumer 1 -> partition 0
Consumer 2 -> partition 1
Consumer 3 -> partition 2
Consumer 4 -> idle
Consumer 5 -> idle
```

We will test this later.

---

# 15. Consumer Group Startup Logs

When `order-notification-service` started, Kafka logged:

```text
Subscribed to topic(s): order-events
```

Then:

```text
Discovered group coordinator localhost:9092
```

The **group coordinator** is the Kafka broker responsible for managing that consumer group.

It helps manage:

- consumer membership
- partition assignment
- group generations/rebalances
- committed offsets

Then we saw:

```text
(Re-)joining group
```

The consumer was joining:

```text
order-notification-group
```

Kafka then assigned:

```text
order-events-0
```

to our consumer.

Since we currently have:

```text
1 partition
1 consumer
```

the assignment is straightforward:

```text
order-notification-service
        |
        v
order-events partition 0
```

---

# 16. Rebalancing

When consumers join or leave a group, Kafka may need to redistribute partitions.

This is called a **rebalance**.

Examples that may trigger a rebalance:

- consumer starts
- consumer stops
- consumer crashes
- new consumer instance joins
- topic partition count changes

We also saw a Kafka informational message about KIP-848 and the newer consumer rebalance protocol.

We have not enabled it yet.

Later we will test consumer scaling and rebalance behavior deliberately.

---

# 17. Kafka Offset

An offset is the position of a record **inside one partition**.

Example:

```text
order-events
partition 0

offset 0 -> Order 3
offset 1 -> Order 4
offset 2 -> Order 5
offset 3 -> Order 6
```

Important:

```text
offset != number of times a record was consumed
```

The same record keeps the same offset even if it is delivered/retried multiple times.

Example:

```text
offset 4 -> Order 7
```

If processing fails 10 times:

```text
attempt 1 -> offset 4
attempt 2 -> offset 4
attempt 3 -> offset 4
...
```

It is still the same Kafka record.

Offsets are unique only inside a partition.

This is valid:

```text
partition 0 -> offset 5
partition 1 -> offset 5
```

---

# 18. Consumer Group Committed Offset

Kafka also tracks where a **consumer group** has progressed.

Example:

```text
order-notification-group
    |
    +---- order-events partition 0
            current offset = 2
```

If the consumer stops and later restarts, Kafka can continue from the consumer group's committed position.

This is different from the offset stored on an individual Kafka record.

---

# 19. Consumer Lag Experiment

We deliberately stopped only:

```text
order-notification-service
```

while keeping:

```text
order-event-service        RUNNING
Kafka                      RUNNING
```

Then we created two more orders.

Kafka stored those records even though the consumer was offline.

Consumer group output:

```text
CURRENT-OFFSET = 2
LOG-END-OFFSET = 4
```

Therefore:

```text
LAG = 4 - 2 = 2
```

Mental model:

```text
Partition 0

offset 0 -> processed
offset 1 -> processed

offset 2 -> waiting
offset 3 -> waiting

LOG-END-OFFSET = 4
```

Important detail:

```text
CURRENT-OFFSET = 2
```

means offset `2` is the next position for the group.

After restarting `order-notification-service`, it consumed the waiting records and the lag returned to zero.

This demonstrated:

```text
consumer goes down
      |
      v
producer continues
      |
      v
Kafka keeps records
      |
      v
consumer group lag grows
      |
      v
consumer restarts
      |
      v
continues from committed position
      |
      v
catches up
```

---

# 20. Useful Consumer Group Command

To inspect the notification consumer group:

```bash
docker exec -it kafka-poc \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group order-notification-group
```

Important columns:

```text
CURRENT-OFFSET
LOG-END-OFFSET
LAG
```

Conceptually:

```text
LAG = LOG-END-OFFSET - CURRENT-OFFSET
```

---

# 21. Consumer Failure Experiment

**Status: DONE**

We deliberately added:

```java
if (event.productId().equals(9999L)) {
    throw new RuntimeException("Notification service failed");
}
```

Then produced an event with:

```json
{
  "customerId": 101,
  "productId": 9999,
  "quantity": 1
}
```

The consumer received:

```text
key=7
partition=0
offset=4
productId=9999
```

Then it failed.

Spring Kafka repeatedly sought back to:

```text
offset 4
```

and retried the same record.

Observed pattern:

```text
Received offset=4
failure
Seeking to offset 4

Received offset=4
failure
Seeking to offset 4

Received offset=4
failure
...
```

This proved directly:

```text
same Kafka record
same partition
same offset
multiple processing attempts
```

Eventually the retry policy was exhausted.

Observed log:

```text
Backoff ... exhausted for order-events-0@4
```

This is our first practical example of **at-least-once processing behavior**.

A consumer must be prepared for the same record to be processed more than once.

---

# 22. Next Step: Verify Progress After Retry Exhaustion

The failure behavior has been confirmed:

- the same record was retried at the same offset
- the retry policy was exhausted
- the failed record did not advance during processing attempts

The next experiment is to publish a normal order after the failed event and observe
whether the consumer moves to the next offset or remains blocked by the failed record.

Send:

```json
{
  "customerId": 101,
  "productId": 5005,
  "quantity": 1
}
```

Expected observation:

```text
partition=0
offset=5
Notification: Order ... placed successfully!
```

This will tell us whether the current exhausted-retry behavior allows later records
to continue or whether the failed record blocks the partition. After this experiment,
we will configure explicit retry handling and a dead-letter topic.

# 23. Important Problem We Have Not Fixed Yet

Current producer code:

```java
@Transactional
public OrderResponse createOrder(...) {

    orderRepository.save(order);

    kafkaTemplate.send(...);
}
```

It may look like PostgreSQL + Kafka are inside one transaction.

They are not automatically one atomic distributed transaction.

This will be one of the most important experiments later.

We want to test scenarios such as:

```text
DB succeeds
Kafka fails
```

and:

```text
Kafka succeeds
DB transaction later rolls back
```

This will lead us toward the **Transactional Outbox Pattern**.

---

# 24. PostgreSQL Sequence Observation

During an earlier failed request, we observed that an order ID could be skipped.

Example:

```text
1
3
4
```

with no row `2`.

This can happen because PostgreSQL sequences/identity values are not rolled back when the surrounding transaction rolls back.

A missing numeric ID does not necessarily mean data was deleted.

---

# 25. Current End-to-End State

We have successfully built and tested:

```text
Postman
   |
   v
POST /api/orders
   |
   v
order-event-service
   |
   +---- save to PostgreSQL
   |
   +---- create OrderPlacedEvent
   |
   +---- publish to Kafka
              |
              v
         order-events
              |
              v
       partition 0
              |
              v
order-notification-group
              |
              v
order-notification-service
              |
              v
         @KafkaListener
              |
              v
"Order placed successfully"
```

We have also verified:

- broker startup
- topic creation
- topic description
- leader broker
- partition
- key
- JSON value
- serialization/deserialization
- consumer group
- group coordinator
- rebalance
- committed offset
- consumer lag
- recovery after consumer downtime
- repeated delivery of the same record
- retry exhaustion

---

# 26. Normal Record After Failure Experiment

**Status: DONE**

Before adding DLT/DLQ, send one normal order **after the failed productId=9999 event**.

For example:

```json
{
  "customerId": 101,
  "productId": 5005,
  "quantity": 1
}
```

Observe whether the consumer processes the next Kafka record.

We want to see something similar to:

```text
partition=0
offset=5
Notification: Order ... placed successfully!
```

---

# 27. Failed Record Progress Experiment

**Status: DONE**

## Question: What happened after the failed record exhausted its retries?

The consumer group reported:

```text
CURRENT-OFFSET = 6
LOG-END-OFFSET = 6
LAG = 0
```

Observed partition state:

```text
offset 4 -> productId=9999, failed repeatedly
offset 5 -> normal order, processed successfully
next position -> offset 6
```

The normal event was processed, so the failed record at offset `4` did not
permanently block the partition. After retry exhaustion, Spring Kafka's default
error handler recovered/skipped the failed record and allowed the consumer group
to continue with offset `5`.

## Question: Did Kafka delete the failed message at offset 4?

No. Kafka did not remove the record or renumber later records:

```text
offset 4 -> failed event, still stored
offset 5 -> next event, still stored
```

The record remains in the Kafka log until normal retention removes it. It can
still be read by another consumer group or replayed by resetting the consumer
group offset.

## Question: What changed after retry exhaustion?

The consumer group's committed position advanced. `CURRENT-OFFSET = 6` means
that this consumer group will next read from offset `6`; it does not mean that
offset `4` was deleted.

Kafka record offsets are immutable, append-only positions. The next event is
read from offset `5`, not from `4`, because the error handler allowed the group
to move past the failed record.

## Question: What is the problem with the current behavior?

The failed notification is effectively forgotten by the main consumer after
retry exhaustion. There is no explicit place to inspect, alert on, or replay the
failed business event.

## Next step: Dead Letter Topic

Add explicit Spring Kafka failure handling:

```text
DefaultErrorHandler
        -> retry and backoff

DeadLetterPublishingRecoverer
        -> publish unrecoverable records

order-events.DLT
        -> retain failed events for inspection/replay
```

Expected flow:

```text
order-events
     |
     v
notification listener
     |
  success -----------------> notification sent
     |
  failure -> retry -> exhausted -> order-events.DLT
```

The `productId == 9999` failure condition should remain enabled for the DLT
experiment. The DLT record should be inspected together with its key, payload,
original topic, partition, offset, and exception metadata.

---

# 28. Dead Letter Topic (DLT) Implementation

**Status: DONE**

We introduced a Dead Letter Topic (`order-events.DLT`) and configured Spring Kafka's error handling to prevent poison-pill records from blocking consumer partitions while preserving unprocessable messages for inspection, alerting, or replay.

### Target Architecture

```text
order-events (Main Topic)
      |
      v
order-notification-service
      |
      |-- Success -------------------------------------> Notification Sent
      |
      |-- Failure (e.g. productId == 9999)
             |
             v
      DefaultErrorHandler (FixedBackOff: 1000ms interval, 2 retries)
             |
             |-- Attempt 1 (Initial): Fails
             |-- Attempt 2 (Retry 1): Fails (after 1s)
             |-- Attempt 3 (Retry 2): Fails (after 1s)
             |
             v (Retries Exhausted)
      DeadLetterPublishingRecoverer
             |
             v
      order-events.DLT (Dead Letter Topic)
             |
             v
      Main partition offset commits -> Next messages (e.g. offset 5) continue
```

---

### Implementation Details

#### 1. Error Handler Configuration (`KafkaErrorHandlerConfig.java`)

```java
@Configuration
public class KafkaErrorHandlerConfig {

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            KafkaTemplate<Object, Object> kafkaTemplate) {

        return new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) ->
                        new TopicPartition(
                                record.topic() + ".DLT",
                                record.partition()
                        )
        );
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(
            DeadLetterPublishingRecoverer recoverer) {

        return new DefaultErrorHandler(
                recoverer,
                new FixedBackOff(1000L, 2L)
        );
    }
}
```

- **`DefaultErrorHandler`**: Intercepts listener exceptions and executes retry logic according to `FixedBackOff(1000L, 2L)` (interval: 1000ms, retry count: 2, total deliveries: 3).
- **`DeadLetterPublishingRecoverer`**: When retries are exhausted, it forwards the failed record to `order-events.DLT` on the matching partition using `KafkaTemplate`.

#### 2. Producer Serializer Configuration (`application.properties`)

Because `DeadLetterPublishingRecoverer` uses a `KafkaTemplate` to publish the failed event to the DLT, the notification service must configure producer serializers:

```properties
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.LongSerializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JacksonJsonSerializer
```

---

### Key Observations & Behavior

1. **Retry Cycle**:
   - For an event with `productId=9999L`, the listener fails with `RuntimeException("Notification service failed")`.
   - The message is retried 2 times with a 1-second pause between attempts.
2. **Publishing to DLT**:
   - Once all retries are exhausted, the record is published to `order-events.DLT`.
   - The original key (`Long`) and JSON payload are preserved.
3. **Offset Progression (No Poison Pill)**:
   - The consumer group's committed offset on `order-events` moves forward past the failed offset.
   - Subsequent records on the same partition (e.g., offset 5) are processed without disruption.
4. **DLT Headers Added by Spring Kafka**:
   Spring Kafka enriches the published dead-letter record with diagnostic headers:
   - `kafka_dlt-original-topic`: Name of the source topic (`order-events`).
   - `kafka_dlt-original-partition`: Source partition index.
   - `kafka_dlt-original-offset`: Source offset of the failed message.
   - `kafka_dlt-original-timestamp`: Timestamp of the original event.
   - `kafka_dlt-exception-fqcn`: Fully-qualified class name of the thrown exception (`java.lang.RuntimeException`).
   - `kafka_dlt-exception-message`: Error message (`Notification service failed`).
   - `kafka_dlt-exception-stacktrace`: Full exception stacktrace.

---

### Useful Commands to Verify DLT

#### List Topics (Verify `order-events.DLT` exists)

```bash
docker exec -it kafka-poc \
  /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

#### Consume from DLT with Headers and Key

```bash
docker exec -it kafka-poc \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --topic order-events.DLT \
  --bootstrap-server localhost:9092 \
  --property print.key=true \
  --property print.headers=true \
  --property key.separator=" -> " \
  --property key.deserializer=org.apache.kafka.common.serialization.LongDeserializer \
  --from-beginning
```

---

# 29. Duplicate Delivery & Offset Replay Experiment

**Status: DONE**

We simulated duplicate event delivery by resetting the consumer group's committed offset backward on the broker, demonstrating why Kafka's at-least-once delivery guarantees mandate idempotent business consumers.

### Experiment Procedure & Observations

1. **Initial Baseline Processing**:
   - Produced a normal order (`orderId = 12`).
   - Notification service consumed the event:
     ```text
     Received event | key=12 | partition=0 | offset=9 | orderId=12
     Notification: Order 12 placed successfully!
     ```
   - Partition position committed to offset `10`.

2. **Consumer Shutdown & State Inspection**:
   - Stopped `order-notification-service`.
   - Verified consumer group status:
     ```bash
     docker exec -it kafka-poc \
       /opt/kafka/bin/kafka-consumer-groups.sh \
       --bootstrap-server localhost:9092 \
       --describe \
       --group order-notification-group
     ```
     Output:
     ```text
     GROUP                    TOPIC        PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID  HOST  CLIENT-ID
     order-notification-group order-events 0          10              10              0    -            -     -
     ```
     `CURRENT-OFFSET = 10` confirms that offset `9` was processed and committed, and the next read position is `10`.

3. **Intentionally Shifting Offset Backward**:
   - Executed offset reset with `--shift-by -1`:
     ```bash
     docker exec -it kafka-poc \
       /opt/kafka/bin/kafka-consumer-groups.sh \
       --bootstrap-server localhost:9092 \
       --group order-notification-group \
       --topic order-events:0 \
       --reset-offsets \
       --shift-by -1 \
       --execute
     ```
     Result:
     ```text
     GROUP                     TOPIC          PARTITION   NEW-OFFSET
     order-notification-group  order-events   0           9
     ```
     Kafka now views offset `9` as unconsumed for `order-notification-group`.

4. **Service Restart & Duplicate Execution**:
   - Restarted `order-notification-service`.
   - Consumer resumed from the newly reset offset `9`:
     ```text
     Received event | key=12 | partition=0 | offset=9 | orderId=12
     Notification: Order 12 placed successfully!
     ```
   - Result: The exact same business event produced a duplicate notification.

---

### Core Learning & Mental Model

```text
First processing:
Offset 9 (Order 12) ----> Processed ----> Send Notification ✅ ----> Commit offset 10

Offset Reset (--shift-by -1):
Committed position moved 10 -> 9

Consumer Restart:
Offset 9 (Order 12) ----> Delivered again ----> Send Notification AGAIN ❌ (Duplicate side-effect)
```

1. **Kafka Replay vs. New Record**:
   - Kafka did not create a new event. The record at `partition=0, offset=9` was simply re-read.
   - Offsets are immutable; consumer group progress is just a pointer (`committed offset`) stored in Kafka's internal `__consumer_offsets` topic.
2. **At-Least-Once Delivery**:
   - In real-world systems, duplicate deliveries occur regularly due to network timeouts during offset commit, consumer restarts/crashes before commit, rebalances, or administrative re-processing.
   - At-least-once delivery + non-idempotent consumer = duplicate side effects (duplicate emails, double billing, duplicate notifications).

---

### Planned Idempotency Evolution

```text
Order event arrives
       |
       v
Have I already processed OrderPlaced for this orderId?
       |
   +---+---+
   |       |
  NO      YES
   |       |
   v       v
Process & Send    Ignore Duplicate
Notification
```

#### Step 1: Naive In-Memory Deduplication (Upcoming Experiment)
- Implement `ConcurrentHashMap.newKeySet()` (`Set<Long> processedOrders`).
- Intended Flaws to Observe:
  1. **JVM Restart Loss**: On service restart, the in-memory set resets; replayed events will be reprocessed.
  2. **Multi-Instance Isolation**: Multiple instances have independent memory sets; cannot coordinate duplicate suppression across instances.

#### Step 2: Persistent Deduplication Store (Production Pattern)
- A persistent database table (`processed_events` or `idempotency_keys`) with a `UNIQUE` constraint on `order_id` / `event_id`.
- Ensures atomic, durable, multi-instance deduplication.

---

# 30. Planned Learning Roadmap

After DLT and Duplicate Delivery, continue roughly in this order:

```text
1. Explicit retry configuration              ✅
2. Dead Letter Topic (DLT)                   ✅
3. Duplicate delivery via offset reset       ✅
4. In-memory idempotent consumer (naive)     ⏳ next
5. Breaking in-memory idempotency            ⏳ upcoming
6. Persistent idempotency (DB table)         ⏳ upcoming
7. Retryable vs non-retryable exceptions
8. Manual / different acknowledgement modes
9. Multiple partitions
10. Multiple instances of notification service
11. Consumer group load balancing
12. Consumer rebalance
13. Kafka key and ordering
14. Multiple brokers
15. Replication factor
16. Leader / follower failover
17. Producer acknowledgements (acks)
18. Idempotent producer
19. Kafka delivery semantics
20. PostgreSQL + Kafka failure window
21. Transactional Outbox
22. Outbox publisher
23. Eventual consistency
24. OpenSearch read model
25. Redis where useful
26. Observability / correlation ID
27. Docker Compose
28. Kubernetes deployment
```

---

# 31. Multi-Partition Experiment Planned

Later change:

```text
order-events
1 partition
```

to multiple partitions.

For example:

```text
order-events
├── partition 0
├── partition 1
└── partition 2
```

Because our key is:

```text
orderId
```

Kafka will use the serialized key when deciding the partition.

We will test:

- which order IDs go to which partitions
- same key -> same partition
- ordering inside one partition
- lack of global ordering across partitions
- what happens if partition count changes

---

# 32. Multi-Consumer Experiment Planned

With 3 partitions:

```text
order-events
├── P0
├── P1
└── P2
```

Start multiple instances of:

```text
order-notification-service
```

using the same:

```text
groupId=order-notification-group
```

Expected idea:

```text
instance 1 -> P0
instance 2 -> P1
instance 3 -> P2
```

Then start a fourth instance and observe that it may remain idle.

Stop one consumer and observe the rebalance.

---

# 33. Multi-Broker Experiment Planned

Later run multiple Kafka brokers.

Example:

```text
Broker 1
Broker 2
Broker 3
```

Then create partitions and replicas.

Observe:

```text
Partition 0
  leader -> Broker 1
  replicas -> 1,2,3

Partition 1
  leader -> Broker 2
  replicas -> 2,3,1
```

Then stop a leader broker and observe leader election / availability behavior.

This will make the concepts of:

- bootstrap servers
- metadata
- partition leader
- follower
- replica
- ISR
- replication factor

practical rather than theoretical.

---

# 34. OpenSearch Plan

PostgreSQL will remain the source of truth.

Later:

```text
PostgreSQL
    |
    | domain events
    v
Kafka
    |
    +----> notification consumer
    |
    +----> search-index consumer
              |
              v
          OpenSearch
```

Possible read endpoints:

```text
GET /orders/search?q=...
GET /orders/customer/{customerId}
GET /orders?status=PLACED
```

This will introduce:

- separate read model
- eventual consistency
- CQRS-style thinking
- rebuilding an index from events
- handling stale search data

---

# 35. Core Mental Models So Far

## Broker

```text
Kafka server/node
```

## Topic

```text
logical stream of events
```

## Partition

```text
ordered append-only log inside a topic
```

## Key

```text
used by the producer partitioning logic
```

## Offset

```text
position of a record inside a partition
```

## Consumer Group

```text
set of consumers cooperating to share partitions
```

## Committed Offset

```text
consumer group's progress for a partition
```

## Lag

```text
how far a consumer group is behind the end of the log
```

## Rebalance

```text
redistribution of partitions among consumers in a group
```

## Serializer

```text
Java object -> byte[]
```

## Deserializer

```text
byte[] -> Java object
```

## DefaultErrorHandler

```text
Spring Kafka listener error handler managing retry attempts and backoff
```

## DeadLetterPublishingRecoverer

```text
routes unrecoverable / retry-exhausted messages to a Dead Letter Topic (DLT)
```

## Dead Letter Topic (DLT)

```text
side topic holding failed messages with diagnostic headers to avoid poison-pill consumer blocking
```

## Offset Reset / Replay

```text
administratively shifting a consumer group's committed offset backward to reprocess earlier records
```

## At-least-once implication

```text
the same Kafka record may be processed more than once due to retries, crashes, or rebalances
```

## Idempotent Consumer

```text
consumer business logic designed to produce the same outcome regardless of how many times a message is delivered
```

---

# 36. Commands Used Frequently

Check Kafka container:

```bash
docker ps
```

Kafka logs:

```bash
docker logs kafka-poc --tail 50
```

List topics:

```bash
docker exec -it kafka-poc \
  /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

Describe topic:

```bash
docker exec -it kafka-poc \
  /opt/kafka/bin/kafka-topics.sh \
  --describe \
  --topic order-events \
  --bootstrap-server localhost:9092
```

Console consumer:

```bash
docker exec -it kafka-poc \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --topic order-events \
  --bootstrap-server localhost:9092 \
  --from-beginning
```

Console consumer showing Long key:

```bash
docker exec -it kafka-poc \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --topic order-events \
  --bootstrap-server localhost:9092 \
  --property print.key=true \
  --property key.separator=" -> " \
  --property key.deserializer=org.apache.kafka.common.serialization.LongDeserializer \
  --from-beginning
```

Console consumer for DLT with headers:

```bash
docker exec -it kafka-poc \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --topic order-events.DLT \
  --bootstrap-server localhost:9092 \
  --property print.key=true \
  --property print.headers=true \
  --property key.separator=" -> " \
  --property key.deserializer=org.apache.kafka.common.serialization.LongDeserializer \
  --from-beginning
```

Describe consumer group:

```bash
docker exec -it kafka-poc \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group order-notification-group
```

Reset consumer group offset (shift backward by 1 record):

```bash
docker exec -it kafka-poc \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group order-notification-group \
  --topic order-events:0 \
  --reset-offsets \
  --shift-by -1 \
  --execute
```

Check Colima:

```bash
colima status
```

Start Colima:

```bash
colima start
```

Stop Colima:

```bash
colima stop
```

---

# 37. Current Checkpoint

At this checkpoint:

```text
Producer works                     ✅
PostgreSQL persistence works       ✅
Kafka broker works                 ✅
Kafka topic works                  ✅
Long key works                     ✅
JSON event works                   ✅
Real consumer works                ✅
Consumer group works               ✅
Offset observed                    ✅
Lag observed                       ✅
Offline consumer recovery          ✅
Failure simulation                 ✅
Same-offset retries observed       ✅
Dead Letter Topic (DLT)            ✅
Duplicate Delivery (Offset Reset)  ✅

In-Memory Idempotency (Naive)      ⏳ next
Persistent Idempotency (DB Table)  ⏳ upcoming
Retryable vs Non-retryable Errors  ⏳ upcoming
Multiple partitions                ⏳ later
Multiple consumers                 ⏳ later
Multiple brokers                   ⏳ later
Outbox                             ⏳ later
OpenSearch                         ⏳ later
```
