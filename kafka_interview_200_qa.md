# 200 Apache Kafka Interview Questions for Data Engineers (2 Years Experience)

Real-industry, scenario-driven questions with answers an experienced professional would give.

## Table of Contents

| Section | Questions |
|---|---|
| 1. Kafka Fundamentals | Q1 – Q25 |
| 2. Architecture: Brokers, Partitions, Replication, KRaft, Storage | Q26 – Q50 |
| 3. Producers | Q51 – Q80 |
| 4. Consumers & Consumer Groups | Q81 – Q110 |
| 5. Delivery Semantics, Idempotence & Transactions | Q111 – Q125 |
| 6. Schema Registry & Data Contracts | Q126 – Q135 |
| 7. Kafka Connect & CDC | Q136 – Q150 |
| 8. Kafka Streams, ksqlDB, Spark/Flink with Kafka | Q151 – Q170 |
| 9. Operations, Security, Monitoring & Performance | Q171 – Q188 |
| 10. Real Industry Scenarios | Q189 – Q200 |

> Defaults mentioned are for Apache Kafka 3.x/4.x. Always confirm against your cluster's version, because several defaults changed (for example, producer `acks=all` and idempotence became default in 3.0, and ZooKeeper was removed in 4.0).

---

# SECTION 1: KAFKA FUNDAMENTALS

### Q1. What is Apache Kafka?
**Answer:** A distributed, durable, high-throughput event streaming platform. It stores streams of records in topics as a replicated, append-only commit log, lets many producers write and many consumers read independently, and retains data for a configurable time so it can be replayed. It is used for messaging, log and event aggregation, data pipelines, stream processing, and event-driven microservices.

---

### Q2. Explain the core Kafka terms: broker, topic, partition, offset, producer, consumer, consumer group.
**Answer:**
- **Broker**: a Kafka server that stores partitions and serves clients.
- **Topic**: a named stream of records, split into partitions.
- **Partition**: an ordered, immutable log; the unit of parallelism and replication.
- **Offset**: a record's sequential position inside a partition.
- **Producer**: writes records to topics.
- **Consumer**: reads records from topics.
- **Consumer group**: consumers sharing a `group.id` that divide a topic's partitions among themselves.

---

### Q3. Kafka vs a traditional message queue (RabbitMQ, ActiveMQ)?
**Answer:**
| Aspect | Kafka | Traditional MQ |
|---|---|---|
| Storage | Retains messages by time/size; replayable | Usually deleted after consumption |
| Consumption | Consumers pull and track their own offsets | Broker pushes and tracks delivery |
| Throughput | Millions of msgs/sec, sequential disk I/O | Lower, optimized for routing |
| Ordering | Per partition | Per queue |
| Scaling | Partitions across brokers | Competing consumers on queues |
| Routing | Simple (topic/partition) | Rich (exchanges, routing keys) |

Pick Kafka for high-volume streaming and replay; pick a classic MQ for complex routing and per-message acknowledgements and priorities.

---

### Q4. Why is Kafka so fast?
**Answer:** Sequential disk writes (append-only log), the OS page cache for reads and writes, zero-copy transfer (`sendfile`) from disk to network, batching and compression of records, partition-level parallelism, and a simple pull-based consumer model with no per-message broker bookkeeping.

---

### Q5. What is a partition and why does it matter?
**Answer:** A topic is divided into partitions; each partition is an ordered log stored on one leader broker (and replicated). Partitions give parallelism (consumers in a group read different partitions), scalability (spread across brokers), and ordering (guaranteed only within one partition). The partition count caps the maximum consumer parallelism in a group.

---

### Q6. What is an offset?
**Answer:** A monotonically increasing number identifying a record within a partition. Offsets are unique only per partition. A consumer tracks "the next offset to read"; committing offsets stores that position (in the `__consumer_offsets` topic) so it can resume after a restart.

---

### Q7. What ordering guarantees does Kafka give?
**Answer:** Records are ordered within a single partition. There is no ordering guarantee across partitions of a topic. To keep all events of an entity (such as one order) in order, use the entity ID as the record key so they hash to the same partition.

---

### Q8. What does a Kafka record contain?
**Answer:** A key (optional), a value, headers (optional key-value metadata), a timestamp (create time or log append time), and, once stored, its partition and offset. Keys drive partitioning and compaction; headers carry metadata such as trace IDs and schema info without changing the payload.

---

### Q9. Typical Kafka use cases in data engineering?
**Answer:** Ingesting application events and logs, CDC streams from databases, real-time ETL into a lake or warehouse, feeding Spark/Flink/Kafka Streams jobs, metrics and monitoring pipelines, event sourcing and microservice communication, IoT telemetry, and clickstream analytics.

---

### Q10. Explain the publish-subscribe and queue models in Kafka.
**Answer:** Consumers in different groups each receive all records of a topic (pub-sub). Consumers inside the same group split the partitions, so each record goes to only one consumer in that group (queue-like load sharing). One topic can serve both styles at the same time.

---

### Q11. Is Kafka a database?
**Answer:** It is a durable log with retention and compaction, and it can serve as a source of truth for event streams, but it is not a general-purpose database. It lacks ad hoc queries, secondary indexes, and row-level transactions across topics. Kafka Streams state stores and ksqlDB add queryable state, but analytical queries normally go to a lake, warehouse, or OLAP store.

---

### Q12. Kafka vs Pulsar vs Kinesis vs SQS vs Google Pub/Sub?
**Answer:** Kafka: open source, highest ecosystem maturity, broker-coupled storage (tiered storage available). Pulsar: separates compute from storage (BookKeeper), native multi-tenancy and geo-replication. Kinesis: AWS-managed, shard-based, simpler operations, limits per shard. SQS: simple managed queue, no replay, no ordering unless FIFO. Pub/Sub: managed, global. Choose by operations capacity, ecosystem (Connect, Streams), and cloud alignment.

---

### Q13. Streaming vs batch: when do you choose Kafka-based streaming?
**Answer:** Choose streaming when the business needs low latency (fraud detection, live dashboards, alerts), continuous ingestion, or multiple consumers of the same events. Choose batch when daily/hourly freshness is acceptable and simplicity and cost matter. Many systems are hybrid: Kafka for ingestion and near-real-time, with batch jobs for heavy recomputation or corrections.

---

### Q14. List the five core Kafka APIs.
**Answer:** Producer API, Consumer API, Streams API (stream processing library), Connect API (source/sink integrations), and Admin API (manage topics, ACLs, configs, consumer groups).

---

### Q15. Does Kafka delete a message once a consumer reads it?
**Answer:** No. Records stay until retention expires (time- or size-based) or they are removed by compaction, regardless of consumption. This allows replay, multiple independent consumers, and reprocessing after bug fixes.

---

### Q16. Push vs pull: why do Kafka consumers pull?
**Answer:** Pull lets each consumer control its own rate (natural backpressure), batch efficiently, and rewind offsets freely. A push system can overwhelm a slow consumer. Long polling (`fetch.min.bytes`, `fetch.max.wait.ms`) avoids busy loops when there is no data.

---

### Q17. What was ZooKeeper's role and what replaced it?
**Answer:** ZooKeeper stored cluster metadata, handled controller election, and tracked broker membership. KRaft (Kafka Raft) moves metadata management into Kafka itself using a Raft-based quorum of controllers and stores metadata in an internal log. KRaft became production-ready in 3.3 and ZooKeeper support was removed in Kafka 4.0, so new clusters use KRaft only.

---

### Q18. Create a topic and inspect it using the CLI.
**Answer:**
```bash
kafka-topics.sh --bootstrap-server broker1:9092 --create \
  --topic orders --partitions 6 --replication-factor 3 \
  --config retention.ms=604800000 --config min.insync.replicas=2

kafka-topics.sh --bootstrap-server broker1:9092 --describe --topic orders
kafka-topics.sh --bootstrap-server broker1:9092 --list
```
`--describe` shows the leader, replicas, and ISR for every partition.

---

### Q19. Produce and consume from the console.
**Answer:**
```bash
kafka-console-producer.sh --bootstrap-server broker1:9092 --topic orders \
  --property parse.key=true --property key.separator=:
kafka-console-consumer.sh --bootstrap-server broker1:9092 --topic orders \
  --from-beginning --group debug-1 --property print.key=true --property print.timestamp=true
```

---

### Q20. What happens when a record has no key vs a key?
**Answer:** With a key, the default partitioner hashes it (murmur2) and maps it to a partition, so the same key always lands in the same partition (as long as the partition count doesn't change). With a null key, the producer's sticky partitioner fills a batch for one partition and then switches to another, spreading data evenly while keeping batches large.

---

### Q21. What are serializers and deserializers?
**Answer:** Producers convert keys and values to bytes with serializers (`StringSerializer`, `LongSerializer`, Avro, Protobuf, JSON); consumers reverse that with matching deserializers. A mismatch causes errors or garbage data, so producers and consumers need an agreed format, ideally enforced with Schema Registry.

---

### Q22. What is the maximum message size, and how do you send larger payloads?
**Answer:** The default limit is about 1 MB (`message.max.bytes` on the broker, `max.request.size` on the producer, `max.partition.fetch.bytes`/`fetch.max.bytes` on the consumer). Raising limits requires consistent changes everywhere and hurts latency and memory. Better: store the large object (file, image) in S3/HDFS and send a reference plus metadata through Kafka, or split into chunks.

---

### Q23. What is a commit log?
**Answer:** An append-only, ordered sequence of records where new data is only added at the end and existing entries are never modified. A Kafka partition is exactly this, which makes replication (copy the log), recovery (replay), and consumption (track a position) simple and reliable.

---

### Q24. What are the three kinds of time in streaming?
**Answer:** Event time (when the event happened at the source), ingestion/log-append time (when the broker stored it), and processing time (when the application handles it). Use event time for correct windowing and aggregations with late data; processing time is simple but varies with delays and reprocessing. Topic setting `message.timestamp.type` chooses `CreateTime` or `LogAppendTime`.

---

### Q25. How do you decide between one topic with many event types or one topic per event type?
**Answer:** Use one topic per entity or event stream when consumers care about specific types and schemas differ a lot (simple schemas, clear ACLs, independent retention). Put multiple event types in one topic when ordering across those types matters for the same key (e.g., `OrderCreated`, `OrderPaid`, `OrderShipped` for one order), using a schema registry with union types or subject name strategies. Avoid too many tiny topics, since each partition adds overhead.

---

# SECTION 2: ARCHITECTURE — BROKERS, PARTITIONS, REPLICATION, KRAFT, STORAGE

### Q26. What does a broker do?
**Answer:** It stores partition logs on disk, handles produce and fetch requests, replicates data to/from other brokers, and participates in metadata and controller functions. Any broker can answer metadata requests, so clients only need a few `bootstrap.servers` to discover the whole cluster.

---

### Q27. What is `bootstrap.servers`?
**Answer:** An initial list of brokers (host:port) the client uses to fetch cluster metadata. After that, the client connects directly to the partition leaders it needs. Provide at least two or three brokers for resilience; you don't need to list all.

---

### Q28. Explain partition leaders and followers.
**Answer:** Each partition has one leader replica that handles all reads and writes (by default), and zero or more follower replicas that continuously fetch from the leader to stay in sync. If the leader fails, one of the in-sync followers becomes the new leader. Leadership is spread across brokers to balance load.

---

### Q29. What is the replication factor and what value do you use in production?
**Answer:** The number of copies of each partition across different brokers. Production standard is 3 (with `min.insync.replicas=2`), tolerating one broker failure with no data loss and no unavailability for `acks=all` producers. Replication factor 1 means any broker loss loses data or availability.

---

### Q30. What is the ISR (in-sync replica set)?
**Answer:** The set of replicas (including the leader) that are fully caught up with the leader (within `replica.lag.time.max.ms`, default 30 seconds). Only ISR members are eligible for leader election by default, and `acks=all` waits for all ISR members to acknowledge. A follower that falls behind is removed from the ISR and rejoins after catching up.

---

### Q31. Explain how `acks`, `replication.factor`, and `min.insync.replicas` work together.
**Answer:** With `acks=all`, the leader acknowledges only after all current ISR members have the record. `min.insync.replicas` sets the minimum ISR size required to accept such writes; if the ISR falls below it, producers get `NotEnoughReplicas` errors instead of silently losing durability. Example: RF=3, min.insync=2, acks=all tolerates one broker down while still being durable; two brokers down makes the partition unavailable for writes (by design).

---

### Q32. What is the high watermark?
**Answer:** The offset up to which all ISR replicas have replicated. Consumers can only read records below the high watermark (committed records), which prevents them from seeing data that could be lost if the leader fails. The last stable offset (LSO) further limits visibility for transactional read-committed consumers.

---

### Q33. What happens when a leader broker fails?
**Answer:** The controller detects the failure (session timeout or lost heartbeat), picks a new leader from the ISR for each affected partition, and publishes the new metadata. Clients refresh metadata and retry against the new leaders. With a healthy ISR, no committed data is lost and the interruption is typically seconds.

---

### Q34. What is unclean leader election?
**Answer:** Electing an out-of-sync replica as leader when no ISR member is available (`unclean.leader.election.enable=true`). It restores availability but can lose committed records and cause log divergence. The default is false; enable it only for use cases that prefer availability over durability (such as non-critical metrics).

---

### Q35. What is the controller?
**Answer:** The role responsible for cluster-wide administrative tasks: electing partition leaders, tracking broker membership, and propagating metadata changes. In ZooKeeper mode one broker is elected controller; in KRaft mode, a quorum of controller nodes (one active leader, others as hot standbys) manages metadata through a Raft log.

---

### Q36. Describe the KRaft architecture and its benefits.
**Answer:** Controller nodes form a Raft quorum (typically 3 or 5) and store metadata in an internal `__cluster_metadata` log; brokers fetch metadata updates from it. Nodes can run as `broker`, `controller`, or both (combined mode, not recommended for production at scale). Benefits: simpler operations (no separate ZooKeeper cluster), faster controller failover, support for many more partitions, and single security model.

---

### Q37. How would you migrate a cluster from ZooKeeper to KRaft?
**Answer:** Upgrade to a bridge release that supports migration (3.7+/3.9), provision a KRaft controller quorum, enable migration mode so metadata is copied from ZooKeeper, roll the brokers to KRaft mode, finalize the migration, and remove ZooKeeper. Test the procedure on a staging cluster, take backups of configs, and monitor under-replicated partitions throughout. Since 4.0 has no ZooKeeper mode, the migration must be completed before upgrading to 4.x.

---

### Q38. What is rack awareness?
**Answer:** Setting `broker.rack` (for example, the AWS availability zone) lets Kafka spread a partition's replicas across racks/AZs so an AZ failure does not take out all replicas. Consumers can use `client.rack` with `replica.selector.class=RackAwareReplicaSelector` to fetch from the closest follower, reducing cross-AZ network cost (follower fetching).

---

### Q39. How do you decide the number of partitions?
**Answer:** Estimate from throughput and consumer parallelism: `partitions >= max(target throughput / per-partition producer throughput, target throughput / per-partition consumer throughput)`. Also consider the desired number of consumers in the group, key distribution, and headroom for growth (commonly 2–3x). Too many partitions increase broker file handles, replication overhead, end-to-end latency, controller recovery time, and rebalancing cost. A typical starting point is 6–30 for medium topics, adjusted by measurement.

---

### Q40. Can you increase partitions later? What's the catch?
**Answer:** Yes: `kafka-topics.sh --alter --topic orders --partitions 12`. The catch: the key-to-partition mapping changes (`hash(key) % partitions`), so new records of an existing key may land in a different partition than older ones, breaking per-key ordering and any key-based state in consumers. Plan partitions upfront, or create a new topic with more partitions and migrate.

---

### Q41. Can you decrease the number of partitions?
**Answer:** No. Kafka doesn't support reducing partitions. You must create a new topic with fewer partitions and copy the data across (MirrorMaker, Connect, or a custom job), then switch producers and consumers.

---

### Q42. How is data stored on disk?
**Answer:** Each partition is a directory containing log segment files (`.log`) plus an offset index (`.index`), a time index (`.timeindex`), and optionally transaction and snapshot files. The active segment receives appends; older segments are immutable and deleted or compacted according to policy. Segment size is `log.segment.bytes` (default 1 GB) or `segment.ms` for time-based rolling.

---

### Q43. How does retention work?
**Answer:** `cleanup.policy=delete` removes whole segments older than `retention.ms` (default 7 days) or when the partition exceeds `retention.bytes`. Deletion occurs at segment granularity, so data can live somewhat longer than the configured time because the active segment isn't deleted until it rolls. Set per topic to match business needs and storage budgets.

---

### Q44. What is log compaction and when do you use it?
**Answer:** With `cleanup.policy=compact`, Kafka keeps at least the latest record for each key and removes older values for the same key in the background. Use it for changelog/state topics (latest customer profile, product catalog, `__consumer_offsets`, Kafka Streams changelogs) where a consumer should be able to rebuild current state by reading the topic from the start.

---

### Q45. How do you delete a key in a compacted topic?
**Answer:** Send a record with that key and a `null` value (a tombstone). After `delete.retention.ms` (default 24 hours) the tombstone itself is removed during compaction, so consumers must read it within that window to learn the deletion.

---

### Q46. Can a topic use both delete and compact?
**Answer:** Yes: `cleanup.policy=compact,delete`. Kafka keeps the latest value per key but also removes anything older than the retention period. Useful for state topics that should eventually expire inactive keys.

---

### Q47. Explain zero-copy and the page cache in Kafka.
**Answer:** Kafka relies on the OS page cache instead of its own heap cache, so recently written/read data is served from RAM. When sending to consumers, `sendfile` transfers bytes from the page cache directly to the network socket without copying through user space, which is why unencrypted reads are so cheap. With TLS, zero-copy is lost because data must be encrypted in user space.

---

### Q48. How do followers replicate data?
**Answer:** Each follower runs fetcher threads that send fetch requests to the leader, just like consumers, and appends the received records to its own log. The leader tracks each follower's fetch position to advance the high watermark and manage the ISR. Replication throughput is controlled by settings like `num.replica.fetchers` and `replica.fetch.max.bytes`.

---

### Q49. What are `__consumer_offsets` and `__transaction_state`?
**Answer:** Internal compacted topics. `__consumer_offsets` (50 partitions by default) stores committed offsets per group, topic, and partition, and group metadata. `__transaction_state` stores transaction coordinator state for transactional producers. Their replication factor should be 3 in production (`offsets.topic.replication.factor`, `transaction.state.log.replication.factor`).

---

### Q50. Which topic-level configurations do you commonly set?
**Answer:** `retention.ms`, `retention.bytes`, `cleanup.policy`, `min.insync.replicas`, `segment.bytes`/`segment.ms`, `max.message.bytes`, `compression.type`, `message.timestamp.type`, `min.cleanable.dirty.ratio`, and `delete.retention.ms`.
```bash
kafka-configs.sh --bootstrap-server b1:9092 --entity-type topics --entity-name orders \
  --alter --add-config retention.ms=259200000,min.insync.replicas=2
```
Topic overrides take precedence over broker defaults. Disable `auto.create.topics.enable` in production so typos don't create accidental topics.

---

# SECTION 3: PRODUCERS

### Q51. Describe the path of a record inside a Kafka producer.
**Answer:** `send()` → interceptors → key/value **serializers** → **partitioner** picks the partition → record goes into the **RecordAccumulator**, a per-partition batch buffer → a background **Sender** thread groups ready batches by broker and sends produce requests → the broker appends to the leader log, replicates, and acknowledges according to `acks` → the producer completes the callback/future. Batching happens before sending, which is why `batch.size` and `linger.ms` drive throughput.

---

### Q52. Explain `acks=0`, `acks=1`, and `acks=all`.
**Answer:**
- `0`: fire and forget; no acknowledgement. Fastest, can silently lose data.
- `1`: the leader writes to its log and replies; data can be lost if the leader fails before followers copy it.
- `all` (or `-1`): the leader waits for all in-sync replicas. Strongest durability; combine with `min.insync.replicas=2` and RF=3.

`acks=all` is the default since Kafka 3.0.

---

### Q53. Explain `retries` and `delivery.timeout.ms`.
**Answer:** Retries resend a batch after a transient failure. Modern producers default `retries` to a very large number and bound the total effort with `delivery.timeout.ms` (default 2 minutes): the total time allowed from `send()` until success or failure, including batching, retries, and waiting. Rule: `delivery.timeout.ms >= linger.ms + request.timeout.ms`.

---

### Q54. How do `batch.size` and `linger.ms` affect performance?
**Answer:** `batch.size` (default 16 KB) is the maximum bytes per partition batch; `linger.ms` is how long the producer waits to fill a batch before sending (default 0 historically; 5 ms in Kafka 4.0+). Larger batches plus a small linger (5–50 ms) increase throughput and compression ratio at the cost of a little latency. For high-volume pipelines, values like `linger.ms=20` and `batch.size=128KB–512KB` are common.

---

### Q55. Which compression types are supported and how do you choose?
**Answer:** `gzip`, `snappy`, `lz4`, `zstd` (and `none`). Compression is applied per batch by the producer and stays compressed on the broker (if the topic's `compression.type=producer`) until the consumer decompresses it. `lz4` and `snappy` give speed; `zstd` gives the best ratio with good speed; `gzip` has the highest CPU cost. Compression helps most with JSON/text and bigger batches.

---

### Q56. What do `buffer.memory` and `max.block.ms` control?
**Answer:** `buffer.memory` (default 32 MB) is the total memory for unsent records. If the producer generates data faster than it can send, the buffer fills and `send()` blocks for up to `max.block.ms` (default 60 s), then throws a `TimeoutException`. This is how a slow broker pushes backpressure onto the application.

---

### Q57. How can retries break ordering and how is it solved?
**Answer:** If `max.in.flight.requests.per.connection > 1` and batch 1 fails while batch 2 succeeds, a retry of batch 1 lands after batch 2 and reorders messages. Solutions: enable idempotence (ordering is preserved with up to 5 in-flight requests), or set in-flight to 1 (lower throughput).

---

### Q58. What is the idempotent producer?
**Answer:** With `enable.idempotence=true` (default since 3.0), the broker assigns the producer a Producer ID (PID) and tracks sequence numbers per partition; it discards duplicate batches caused by retries. It requires `acks=all`, retries > 0, and in-flight <= 5. Limits: dedup covers a single producer session and single partitions; after a producer restart it gets a new PID, so cross-session or cross-partition atomicity needs transactions.

---

### Q59. Write a Python producer with delivery callbacks.
**Answer:**
```python
import json
from confluent_kafka import Producer

p = Producer({
    "bootstrap.servers": "broker1:9092,broker2:9092",
    "acks": "all",
    "enable.idempotence": True,
    "compression.type": "lz4",
    "linger.ms": 20,
    "batch.size": 131072,
})

def on_delivery(err, msg):
    if err is not None:
        print(f"FAILED {msg.key()}: {err}")          # log, alert, or route to a retry store
    else:
        print(f"OK {msg.topic()}[{msg.partition()}]@{msg.offset()}")

for order in orders:
    p.produce("orders", key=str(order["id"]), value=json.dumps(order), on_delivery=on_delivery)
    p.poll(0)           # serve delivery callbacks
p.flush(30)             # block until everything is delivered
```
Always call `flush()` before exiting, or buffered messages are lost.

---

### Q60. Show a minimal Java producer configuration.
**Answer:**
```java
Properties props = new Properties();
props.put("bootstrap.servers", "broker1:9092");
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("acks", "all");
props.put("enable.idempotence", "true");
props.put("compression.type", "zstd");
props.put("linger.ms", "10");
try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
    producer.send(new ProducerRecord<>("orders", orderId, json),
        (meta, ex) -> { if (ex != null) log.error("send failed", ex); });
}
```

---

### Q61. Synchronous vs asynchronous sends?
**Answer:** Synchronous: `producer.send(record).get()` waits for the acknowledgement, which is simple but very slow (one round trip per message). Asynchronous with a callback is the standard approach: it keeps the pipeline full and handles failures in the callback. Only use synchronous sends for low-volume, must-confirm flows.

---

### Q62. How does the default partitioner work, and how would you write a custom one?
**Answer:** With a key: `murmur2(key) % numPartitions`. Without a key: sticky partitioning (fill a batch for one partition, then switch). A custom `Partitioner` is used for business rules, for example routing VIP customers to dedicated partitions or splitting a hot key across several partitions (`key + "-" + random(0..N)`), at the cost of weaker per-key ordering.

---

### Q63. Scenario: one partition holds 70% of the traffic (hot partition). Why and how do you fix it?
**Answer:** Skewed keys (a big merchant, `null`/default key mapped to one partition, or a low-cardinality key). Diagnose with per-partition offset/size growth. Fixes: choose a higher-cardinality key (order ID rather than country), add a salt or sub-key for hot entities (accepting weaker ordering for them), use a custom partitioner, and increase partitions if it helps dilute. Rebalance consumers so the hot partition isn't paired with other heavy partitions.

---

### Q64. Retriable vs non-retriable producer errors?
**Answer:** Retriable (the producer retries automatically): `NotLeaderOrFollower`, `NetworkException`, `TimeoutException`, `NotEnoughReplicas`. Non-retriable (fail the send): `RecordTooLargeException`, `InvalidTopicException`, `SerializationException`, `TopicAuthorizationException`, `UnknownTopicOrPartition` (when auto-create is off). Your callback should log and route non-retriable failures to a dead letter store.

---

### Q65. Scenario: producer throughput is only 5 MB/s but the network and brokers are idle. What do you tune?
**Answer:** The producer is probably sending tiny batches. Increase `linger.ms` (10–50), `batch.size` (128 KB+), enable compression (`lz4`/`zstd`), avoid synchronous sends, increase `buffer.memory`, use multiple producer instances/threads if CPU-bound on serialization, and check `max.in.flight` isn't 1. Verify with metrics: `batch-size-avg`, `records-per-request-avg`, `request-latency-avg`, `compression-rate-avg`.

---

### Q66. Scenario: how do you configure a producer so no data is lost?
**Answer:** Producer: `acks=all`, `enable.idempotence=true`, large `delivery.timeout.ms`, handle callbacks/flush on shutdown. Topic/broker: `replication.factor=3`, `min.insync.replicas=2`, `unclean.leader.election.enable=false`. Also keep the source of truth until the ack arrives (for DB-to-Kafka flows use the outbox pattern), and monitor for under-replicated partitions.

---

### Q67. What is the `max.in.flight.requests.per.connection`?
**Answer:** The maximum number of unacknowledged requests the producer will send on one connection (default 5). Higher values improve throughput; with idempotence enabled up to 5 is safe for ordering.

---

### Q68. What happens if you don't call `flush()` or `close()` on the producer?
**Answer:** Records still in the accumulator (not yet sent) are lost when the process exits. `flush()` blocks until all buffered records are sent and acknowledged; `close()` flushes and releases resources. Always do this in shutdown hooks (`finally` blocks or signal handlers).

---

### Q69. How do you use headers effectively?
**Answer:** Headers carry metadata outside the payload: trace/correlation IDs, schema version, source system, content type, and event type. They enable routing and filtering without deserializing the value, and they pass through Kafka Streams and Connect with SMTs. Keep them small, since every record carries them.

---

### Q70. Can one producer write to several topics?
**Answer:** Yes. One producer instance is thread-safe and can send to any number of topics; sharing one instance across threads usually performs better than many producers because batches fill faster.

---

### Q71. Producer metrics you monitor?
**Answer:** `record-send-rate`, `record-error-rate`, `record-retry-rate`, `request-latency-avg/max`, `batch-size-avg`, `compression-rate-avg`, `buffer-available-bytes`, `produce-throttle-time-avg` (quota throttling), and `io-wait-ratio`. A rising retry or error rate with latency spikes points to broker or network trouble.

---

### Q72. Explain `request.timeout.ms`.
**Answer:** How long the producer waits for a broker response to a request (default 30 s) before treating it as failed and retrying. It's bounded by the total `delivery.timeout.ms`. Setting it too low in a loaded cluster causes unnecessary retries; too high delays failure detection.

---

### Q73. What are quotas in Kafka?
**Answer:** Limits per client/user: produce and fetch byte rates, plus request-time quotas. A client exceeding its quota is throttled (the broker delays responses), protecting the cluster from noisy neighbors. Configure with `kafka-configs.sh --alter --add-config 'producer_byte_rate=10485760' --entity-type clients --entity-name etl-app`.

---

### Q74. What problems arise from using a poor key choice?
**Answer:** Low-cardinality keys cause hot partitions; keys that change for the same entity break ordering; random keys (UUID per event) distribute well but lose per-entity ordering and compaction value; `null` keys prevent compaction and key-based joins. Choose the business identifier whose events must stay ordered (customer ID, device ID, order ID).

---

### Q75. Producer error `NOT_ENOUGH_REPLICAS`. What does it mean and what do you do?
**Answer:** The ISR is smaller than `min.insync.replicas` while the producer uses `acks=all`, so Kafka refuses writes to protect durability. Check broker health, disk, network, and replication lag (`UnderReplicatedPartitions`), and restore the failed brokers. Don't lower `min.insync.replicas` just to silence the error unless the business accepts weaker durability.

---

### Q76. Producer error `LEADER_NOT_AVAILABLE` or `NOT_LEADER_OR_FOLLOWER`?
**Answer:** The client's cached metadata is stale or an election is in progress (new topic, broker restart, or failover). They're retriable: the producer refreshes metadata (`metadata.max.age.ms`) and retries. Persistent errors mean no leader exists (offline partition) or the topic was just created and not fully available.

---

### Q77. `RecordTooLargeException`: how do you diagnose and fix it?
**Answer:** The record exceeds `max.request.size` (producer), `message.max.bytes`/`max.message.bytes` (broker/topic), or batch limits. Fix by compressing, trimming the payload, storing the large object externally (claim-check pattern) and sending a reference, or raising the limits consistently on producer, topic, and consumer (`max.partition.fetch.bytes`) with awareness of memory impact.

---

### Q78. How do you send data to Kafka from a database reliably (dual-write problem)?
**Answer:** Writing to the DB and Kafka separately risks inconsistency if one fails. Use the **transactional outbox** pattern: write the business row and an outbox event row in one DB transaction, then a CDC tool (Debezium) or a relay publishes outbox rows to Kafka. Consumers handle duplicates idempotently. This gives atomicity without distributed transactions.

---

### Q79. How would you simulate or load test a producer?
**Answer:**
```bash
kafka-producer-perf-test.sh --topic perf-test --num-records 10000000 --record-size 1024 \
  --throughput -1 --producer-props bootstrap.servers=b1:9092 acks=all linger.ms=20 batch.size=131072 compression.type=lz4
```
It reports throughput and latency percentiles. Test with realistic record sizes, compression, and replication so numbers match production.

---

### Q80. What does it mean when `send()` blocks for a long time?
**Answer:** Either metadata isn't available (topic doesn't exist, no leader; blocks up to `max.block.ms`) or the buffer is full because brokers or the network are slower than the production rate. Check broker health, increase buffer or batching, scale producers, throttle upstream, or fail fast with a smaller `max.block.ms` and a retry/fallback path.

---

# SECTION 4: CONSUMERS & CONSUMER GROUPS

### Q81. Describe the consumer poll loop.
**Answer:** A consumer subscribes to topics, then repeatedly calls `poll(timeout)`. Each call fetches records (and sends heartbeats in a background thread), triggers group coordination and rebalances, and returns a batch. The app processes the records and commits offsets. If processing takes longer than `max.poll.interval.ms` between polls, the consumer is considered failed and the group rebalances.

---

### Q82. How are partitions assigned in a consumer group?
**Answer:** The group coordinator (a broker) manages membership; one consumer acts as group leader and computes the assignment with the configured assignor (classic protocol), or the broker computes it (new protocol). Each partition goes to exactly one consumer in the group. With more consumers than partitions, the extras sit idle.

---

### Q83. What is a rebalance and what triggers it?
**Answer:** Redistribution of partitions among group members. Triggers: a consumer joins or leaves, a consumer crashes or misses heartbeats (`session.timeout.ms`) or `max.poll.interval.ms`, partitions are added to a subscribed topic, or the subscription (regex topics) changes. During an eager rebalance consumers stop processing, which creates lag spikes.

---

### Q84. Eager vs cooperative rebalancing?
**Answer:** Eager: all consumers revoke all partitions, then everyone is reassigned (stop-the-world). Cooperative (incremental): only partitions that must move are revoked, so the rest keep processing. Enable with `partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor`. Kafka 4.0 also introduces the new consumer group protocol (KIP-848, `group.protocol=consumer`) with broker-side assignment and no global synchronization barrier.

---

### Q85. List the partition assignors.
**Answer:** `RangeAssignor` (per topic, contiguous ranges; can skew when partition counts don't divide evenly), `RoundRobinAssignor` (even spread), `StickyAssignor` (even and minimizes movement), `CooperativeStickyAssignor` (sticky plus incremental rebalance). Default is `[Range, CooperativeSticky]`.

---

### Q86. What is static group membership?
**Answer:** Setting a unique `group.instance.id` per consumer makes the member identity survive restarts: if the consumer returns within `session.timeout.ms`, it gets its old partitions back without triggering a rebalance. Ideal for rolling deployments and stateful consumers (Kafka Streams).

---

### Q87. Explain `session.timeout.ms`, `heartbeat.interval.ms`, and `max.poll.interval.ms`.
**Answer:**
- `heartbeat.interval.ms` (3 s): how often the background thread signals liveness.
- `session.timeout.ms` (45 s default in 3.x+): if no heartbeat arrives in this time, the consumer is removed.
- `max.poll.interval.ms` (5 min): maximum time between `poll()` calls; exceeding it means processing is stuck, and the consumer leaves the group.

Keep `heartbeat.interval.ms` at about one third of the session timeout.

---

### Q88. Auto-commit vs manual commit?
**Answer:** Auto-commit (`enable.auto.commit=true`, every 5 s) commits offsets of records returned by the last poll, regardless of whether processing finished, so a crash can lose records (committed but unprocessed) or duplicate them. Manual commit after successful processing (`commitSync` or `commitAsync`) provides at-least-once control. Production ETL consumers should use manual commits.

---

### Q89. `commitSync` vs `commitAsync`?
**Answer:** `commitSync` blocks and retries until success or fatal error (safe, slower). `commitAsync` doesn't block, no automatic retry (to avoid committing older offsets after newer ones), and reports through a callback (faster). Common pattern: `commitAsync` in the loop, `commitSync` in a `finally` block on shutdown.

---

### Q90. What does `auto.offset.reset` do?
**Answer:** It defines where to start when the group has no committed offset or the offset is out of range: `latest` (default; only new records), `earliest` (from the start of retained data), or `none` (throw an error). It does not apply when valid committed offsets exist.

---

### Q91. Write a Python consumer with at-least-once semantics.
**Answer:**
```python
from confluent_kafka import Consumer, KafkaException

c = Consumer({
    "bootstrap.servers": "broker1:9092",
    "group.id": "orders-etl",
    "enable.auto.commit": False,
    "auto.offset.reset": "earliest",
    "partition.assignment.strategy": "cooperative-sticky",
})
c.subscribe(["orders"])
batch, BATCH_SIZE = [], 500
try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        batch.append(msg)
        if len(batch) >= BATCH_SIZE:
            write_to_warehouse([m.value() for m in batch])   # idempotent write
            c.commit(asynchronous=False)                     # commit after the write succeeds
            batch.clear()
finally:
    c.close()
```
Commit per batch, not per message. If the app crashes after the write but before the commit, the batch is reprocessed, so the sink must be idempotent.

---

### Q92. What is consumer lag and how do you measure it?
**Answer:** Lag = log end offset − committed (or current) offset, per partition. It shows how far a consumer is behind producers.
```bash
kafka-consumer-groups.sh --bootstrap-server b1:9092 --describe --group orders-etl
```
In production, export lag to Prometheus/Grafana with tools like Burrow, `kafka-lag-exporter`, or Confluent Control Center, and alert on sustained growth, not on a single spike.

---

### Q93. Scenario: consumer lag keeps growing. How do you troubleshoot?
**Answer:**
1. Is the rate of production higher than consumption? Compare produce rate and consume rate.
2. Check the group state: rebalancing loops, crashed consumers, or fewer consumers than partitions.
3. Look for slow processing: a slow sink (DB, API), long GC, heavy transformations, or single-record commits.
4. Check for a stuck partition: a poison message or long retry loop on one record.
5. Fix: optimize/batch the sink, increase consumers (up to the partition count), add partitions if parallelism is capped, tune `max.poll.records` and fetch settings, process in parallel worker threads, or isolate slow work via separate topics.

---

### Q94. What if you have more consumers than partitions?
**Answer:** Extra consumers receive no partitions and stay idle (useful as hot standbys). To scale throughput beyond the partition count, increase partitions (with the key-ordering caveat) or process records in a worker thread pool inside each consumer.

---

### Q95. How do you replay or reset offsets for a consumer group?
**Answer:** Stop the group (it must be inactive), then:
```bash
kafka-consumer-groups.sh --bootstrap-server b1:9092 --group orders-etl --topic orders \
  --reset-offsets --to-datetime 2026-10-24T00:00:00.000 --dry-run
# review, then replace --dry-run with --execute
```
Other options: `--to-earliest`, `--to-latest`, `--to-offset N`, `--shift-by -1000`. Reprocessing must be safe: downstream writes need to be idempotent.

---

### Q96. `subscribe` vs `assign`, and `seek`?
**Answer:** `subscribe` joins a consumer group with automatic partition assignment and rebalancing. `assign` manually picks partitions with no group management (used by tools needing full control). `seek(partition, offset)` repositions the consumer, for example to replay from a specific offset or timestamp (`offsetsForTimes`).

---

### Q97. Which fetch settings influence consumer throughput and latency?
**Answer:** `fetch.min.bytes` (default 1) and `fetch.max.wait.ms` (500 ms): the broker waits until enough data accumulates or the wait expires; larger `fetch.min.bytes` raises throughput and lowers request count. `fetch.max.bytes` and `max.partition.fetch.bytes` bound response sizes. Lower values reduce latency; higher values improve efficiency.

---

### Q98. What does `max.poll.records` control?
**Answer:** The maximum number of records returned per `poll()` (default 500). Lower it when processing each record is slow, so you finish the batch within `max.poll.interval.ms` and avoid kicked-out consumers; raise it for fast, lightweight processing.

---

### Q99. Scenario: a malformed ("poison pill") message crashes the consumer repeatedly. What do you do?
**Answer:** Catch deserialization/processing exceptions per record: log the error with topic/partition/offset, send the raw record and error metadata to a **dead letter topic** (DLT), commit past it, and keep going. Use an error-handling deserializer wrapper, alert when DLT volume rises, and provide a reprocessing tool after the bug or schema issue is fixed. Never let one bad record block a partition indefinitely unless strict ordering demands it.

---

### Q100. Explain the retry topic pattern.
**Answer:** For transient failures (downstream API down), publish the failed record to a retry topic (`orders.retry.5m`, `orders.retry.30m`) and let a consumer reprocess it after a delay, escalating to the dead letter topic after N attempts. It keeps the main consumer flowing and preserves the failure context in headers (attempt count, error, original offset). Note that strict ordering of the affected key is lost during retries.

---

### Q101. How do you make a consumer idempotent?
**Answer:** Because at-least-once delivery can redeliver records, make the effect of processing the same record twice identical to processing it once. Techniques: upsert on a unique business key or `(topic, partition, offset)`, a `processed_events` table with a unique constraint on the event ID, conditional writes (`INSERT ... ON CONFLICT DO NOTHING`, `MERGE`), or naturally idempotent operations like `SET status='PAID'`. Avoid non-idempotent operations such as `balance = balance + x` without a dedupe check.

---

### Q102. Can you process records in parallel inside one consumer without losing per-key ordering?
**Answer:** Yes, by hashing the key to a fixed worker thread (one queue per worker), so records of the same key are always handled sequentially by the same worker. Commit offsets only up to the lowest contiguous completed offset, and `pause()` partitions if the queues back up. This pattern adds complexity, so first try batching, bigger consumers, or more partitions.

---

### Q103. What are `pause()` and `resume()` used for?
**Answer:** `pause(partitions)` stops fetching from the given partitions while the consumer continues to `poll()` and send heartbeats, so it stays in the group. Use it for backpressure (downstream sink slow or circuit breaker open) and resume when ready, avoiding `max.poll.interval.ms` violations.

---

### Q104. What is the group coordinator?
**Answer:** The broker that is the leader of the `__consumer_offsets` partition to which the group ID hashes. It manages membership, heartbeats, rebalances, and offset commits for that group. If it fails, the new leader of that partition takes over.

---

### Q105. Scenario: a consumer group is stuck in constant rebalances ("rebalance storm"). What are the likely causes and fixes?
**Answer:**
- Processing exceeds `max.poll.interval.ms` (increase it, reduce `max.poll.records`, or move heavy work off the poll thread).
- Frequent restarts or autoscaling flapping (use static membership, slower scale-in, and graceful shutdown).
- GC pauses or network issues causing missed heartbeats (tune JVM, increase `session.timeout.ms`).
- Eager assignor with many members (switch to `CooperativeStickyAssignor` or the new group protocol).
- Regex subscriptions on topics that are constantly created.

Check the consumer logs for "member ... has left" or "poll timeout has expired" messages.

---

### Q106. Is `KafkaConsumer` thread-safe?
**Answer:** No. The Java consumer is not thread-safe (the one exception is `wakeup()`). Use one consumer per thread, or one consumer thread that hands records to worker threads. The producer, by contrast, is thread-safe.

---

### Q107. What does `isolation.level` do on a consumer?
**Answer:** `read_uncommitted` (default) returns all records, including those from open or aborted transactions. `read_committed` returns only committed transactional records (and non-transactional ones), reading only up to the Last Stable Offset. Use `read_committed` when consuming topics written with transactions.

---

### Q108. CLI commands for managing consumer groups?
**Answer:**
```bash
kafka-consumer-groups.sh --bootstrap-server b1:9092 --list
kafka-consumer-groups.sh --bootstrap-server b1:9092 --describe --group orders-etl
kafka-consumer-groups.sh --bootstrap-server b1:9092 --describe --group orders-etl --members --verbose
kafka-consumer-groups.sh --bootstrap-server b1:9092 --delete --group old-group
```
`--describe` shows `CURRENT-OFFSET`, `LOG-END-OFFSET`, `LAG`, and the member assigned to each partition.

---

### Q109. How long are committed offsets kept?
**Answer:** After a group becomes empty (no active members), its offsets are retained for `offsets.retention.minutes` (default 7 days). If a consumer comes back after that, its offsets are gone and `auto.offset.reset` applies, which can cause unexpected full re-reads or skipped data after long outages. Consider a longer retention for batch-style consumers that run rarely.

---

### Q110. How do you get effectively-once results when the sink is a relational database?
**Answer:** Store the processed results and the consumed offsets in the same database transaction, and on startup `seek()` to the offsets stored in the DB (instead of relying on Kafka's committed offsets). The write and the offset advance succeed or fail together, so reprocessing never duplicates. This is the standard approach when the sink supports transactions.

---

# SECTION 5: DELIVERY SEMANTICS, IDEMPOTENCE & TRANSACTIONS

### Q111. Explain at-most-once, at-least-once, and exactly-once delivery.
**Answer:**
- **At-most-once**: commit offsets before processing (or `acks=0`); no duplicates, but data can be lost.
- **At-least-once**: process, then commit (or `acks=all` with retries); no loss, but duplicates are possible.
- **Exactly-once**: each record's effect is applied once, achieved through idempotent producers and transactions inside Kafka, and idempotent or transactional sinks outside Kafka.

At-least-once with idempotent processing is the most common production choice.

---

### Q112. How does Kafka achieve exactly-once semantics (EOS)?
**Answer:** Three pieces: (1) the idempotent producer prevents duplicates from retries within a partition; (2) transactions let a producer write to multiple partitions/topics and commit consumer offsets atomically, all-or-nothing; (3) consumers with `isolation.level=read_committed` see only committed data. In Kafka Streams, `processing.guarantee=exactly_once_v2` wires this up automatically.

---

### Q113. How does a transactional producer work?
**Answer:** Set a stable `transactional.id`, then:
```text
initTransactions() → beginTransaction() → send(...) ... → sendOffsetsToTransaction(...) → commitTransaction() | abortTransaction()
```
The transaction coordinator writes markers (COMMIT/ABORT) to the partitions involved, tracked in `__transaction_state`. Consumers in `read_committed` mode skip aborted records.

---

### Q114. Show the consume-transform-produce loop with transactions (Python).
**Answer:**
```python
from confluent_kafka import Consumer, Producer

consumer = Consumer({"bootstrap.servers": "b1:9092", "group.id": "enricher",
                     "enable.auto.commit": False, "isolation.level": "read_committed",
                     "auto.offset.reset": "earliest"})
producer = Producer({"bootstrap.servers": "b1:9092", "transactional.id": "enricher-1"})
consumer.subscribe(["orders"])
producer.init_transactions()

while True:
    msg = consumer.poll(1.0)
    if msg is None or msg.error():
        continue
    producer.begin_transaction()
    try:
        producer.produce("orders_enriched", key=msg.key(), value=enrich(msg.value()))
        producer.send_offsets_to_transaction(
            consumer.position(consumer.assignment()),
            consumer.consumer_group_metadata())
        producer.commit_transaction()
    except Exception:
        producer.abort_transaction()
```
Output records and the consumed offsets commit atomically.

---

### Q115. What is the Last Stable Offset (LSO)?
**Answer:** The offset below which all transactions are resolved (committed or aborted). `read_committed` consumers can read only up to the LSO, so a long-running open transaction delays visibility of later records on that partition. Keep transactions short.

---

### Q116. What is a "zombie" producer and how are they fenced?
**Answer:** An old instance of an application (after a network partition or pause) that still believes it owns a `transactional.id`. Kafka assigns an epoch to each `transactional.id`; when a new instance initializes, the epoch increases, and the broker rejects writes from the older epoch (`ProducerFenced`). This prevents duplicate processing by the old instance.

---

### Q117. What are the limits of Kafka's exactly-once guarantee?
**Answer:** It covers read-process-write cycles entirely within Kafka. External side effects (calling an API, writing to a database, sending an email) are outside the transaction, so they can still happen twice unless the target is idempotent or participates in a transactional pattern. Transactions also add latency and overhead, so use them where correctness requires it.

---

### Q118. How do you get exactly-once effects into a warehouse or data lake?
**Answer:** Make the sink idempotent: upsert or `MERGE` by a unique event key, write files with deterministic names per `(topic, partition, offset range)` so reruns overwrite the same file (the S3 sink connector does this), use table formats with transactional commits (Iceberg/Delta with Spark Structured Streaming checkpoints), and deduplicate by event ID in downstream models.

---

### Q119. Scenario: duplicates appear in the warehouse table. What are the possible causes?
**Answer:** Producer retries without idempotence, consumer reprocessing after a rebalance/crash (commit after write), a connector restart replaying a batch, replays from an offset reset, upstream sending duplicate events (CDC snapshot plus stream overlap), or the sink doing `INSERT` instead of `MERGE`. Fix by enabling idempotence, making sinks upsert-based, carrying a unique event ID, and adding a dedupe step (`ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ts DESC)`).

---

### Q120. Scenario: data appears to be lost between source and the data lake. How do you investigate?
**Answer:** Walk the chain: source count vs producer sends (errors/callbacks ignored? `acks` weak?); topic end offsets vs produced counts; broker events (unclean leader election, retention deleting data before consumers read it, `auto.offset.reset=latest` after offset expiry); consumer commit-before-process (auto-commit) or skipped poison messages; connector/DLQ errors; and sink filters or partition mismatches. Compare counts per partition and time window at each hop to locate where the numbers diverge.

---

### Q121. Scenario: events for the same order arrive out of order. How can that happen and how do you prevent it?
**Answer:** Causes: different keys/partitions for the same order, retries with `max.in.flight > 1` without idempotence, multiple producers racing, partition count increase, or parallel processing inside the consumer. Fix: key by order ID consistently, enable idempotence, keep one writer per entity where possible, avoid changing partition count, and in consumers handle by key. Also carry event timestamps or version numbers so consumers can discard stale updates.

---

### Q122. How do you design the key for an `orders` topic when you also need per-customer analytics?
**Answer:** Key by `order_id` for the order lifecycle topic (all events of one order stay ordered). For customer-level aggregation, either re-key via a stream processing job into a separate `orders_by_customer` topic (repartition) or aggregate downstream in a warehouse. One topic can't be partitioned optimally for two different ordering needs.

---

### Q123. Scenario: payments must never be processed twice, but the consumer can crash mid-way. How do you design this?
**Answer:** Use at-least-once consumption plus an idempotency key: each payment event has a unique `payment_id`; the processor inserts it into a `processed_payments` table (unique constraint) in the same DB transaction that applies the payment, and skips if it exists. Commit offsets after the DB transaction. Optionally, use a transactional outbox for emitting follow-up events. The idempotency key also protects against producer-side duplicates and replays.

---

### Q124. What does `transactional.id` need to satisfy?
**Answer:** It must be unique per logical producer instance and stable across restarts of the same instance (so the epoch fencing works), for example `service-name-partition-N`. Two concurrent instances with the same ID fence each other. In Kafka Streams, this is handled internally.

---

### Q125. What overhead does using transactions introduce?
**Answer:** Extra round trips to the coordinator, commit markers written to each partition, and `read_committed` consumers waiting for the LSO. Throughput impact is usually modest with reasonable commit intervals (for example every 100 ms–1 s), but very tiny transactions (one record each) perform poorly. Batch several records per transaction.

---

# SECTION 6: SCHEMA REGISTRY & DATA CONTRACTS

### Q126. What is Schema Registry and why use it?
**Answer:** A service that stores versioned schemas (Avro, Protobuf, JSON Schema) and assigns each schema an ID. Producers register or look up a schema, and send only the compact schema ID with the record; consumers fetch the schema by ID to deserialize. Benefits: smaller messages, enforced data contracts, controlled schema evolution, and early detection of incompatible changes.

---

### Q127. What does the Confluent wire format look like?
**Answer:** `[magic byte 0x0][4-byte schema ID][serialized payload]`. The consumer reads the ID, retrieves (and caches) the schema from the registry, and then deserializes the payload. Connect converters and Streams serdes use the same format.

---

### Q128. Explain compatibility modes.
**Answer:**
- **BACKWARD** (default): consumers using the new schema can read data written with the previous schema (allowed: delete fields, add optional fields with defaults). Upgrade consumers first.
- **FORWARD**: consumers on the old schema can read new data. Upgrade producers first.
- **FULL**: both directions.
- `*_TRANSITIVE` variants check against all previous versions, not just the latest.
- **NONE**: no checks.

---

### Q129. Avro vs Protobuf vs JSON Schema?
**Answer:** Avro: compact binary, schema required for reading, strong evolution rules, a common choice with Hadoop/Spark/Connect. Protobuf: compact, field numbers allow flexible evolution, strong cross-language code generation, popular for microservices. JSON Schema: human-readable and easy, but larger and weaker typing. Raw JSON without a schema is the riskiest for downstream pipelines.

---

### Q130. What are subject naming strategies?
**Answer:** The rule that maps a record to a registry subject: `TopicNameStrategy` (default: `<topic>-value`/`-key`; one schema per topic), `RecordNameStrategy` (by record type name; multiple types per topic), `TopicRecordNameStrategy` (both). Use record-based strategies when one topic carries several event types.

---

### Q131. Scenario: a producer team wants to rename a field and remove another. How do you manage the change?
**Answer:** Renames and removals of required fields are breaking changes. Use the expand-and-contract approach: add the new field (with a default) while keeping the old one, dual-populate both, migrate consumers, then remove the old field in a later version once nobody reads it (for Avro, rename via `aliases` where consumers support it). For truly breaking changes, create a new topic or subject version (`orders.v2`) and migrate consumers deliberately. Enforce compatibility in CI so incompatible schemas fail before deployment.

---

### Q132. Show an Avro producer with Schema Registry (Python).
**Answer:**
```python
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

schema_str = """
{"type":"record","name":"Order","namespace":"com.shop",
 "fields":[{"name":"order_id","type":"string"},
           {"name":"amount","type":"double"},
           {"name":"coupon","type":["null","string"],"default":null}]}"""

sr = SchemaRegistryClient({"url": "http://schema-registry:8081"})
producer = SerializingProducer({
    "bootstrap.servers": "b1:9092",
    "key.serializer": StringSerializer("utf_8"),
    "value.serializer": AvroSerializer(sr, schema_str),
})
producer.produce("orders", key="o-1", value={"order_id": "o-1", "amount": 99.5, "coupon": None})
producer.flush()
```

---

### Q133. What happens if Schema Registry goes down?
**Answer:** Clients cache schemas locally, so producers and consumers already running with known schemas keep working. Failures occur for new schema registrations/lookups (new deployments, new schema versions, cold-started consumers). Run the registry highly available (multiple instances; its data lives in the `_schemas` Kafka topic), and monitor it.

---

### Q134. What is a data contract?
**Answer:** A formal agreement between data producers and consumers: schema, semantics (field meanings, units, nullability), quality rules (not-null, ranges, uniqueness), SLAs (freshness, volume), ownership, and change process. In Kafka it's implemented with Schema Registry compatibility rules, schema rules (validation/CEL), versioned topics, documentation, and automated checks in CI/CD.

---

### Q135. How do you handle a consumer that sees a record with an unknown schema ID?
**Answer:** The deserializer fails with a "schema not found" error. Causes: pointing at the wrong registry or environment, a producer using a different registry, or a deleted schema. Fix the configuration or restore the schema; route undecodable records to a dead letter topic so the consumer doesn't stall.

---

# SECTION 7: KAFKA CONNECT & CDC

### Q136. What is Kafka Connect?
**Answer:** A framework and runtime for moving data between Kafka and external systems with configuration instead of custom code: **source connectors** pull data into Kafka (databases, files, APIs) and **sink connectors** push data out (S3, HDFS, Elasticsearch, JDBC, Snowflake). It provides scalability, fault tolerance, offset tracking, and a REST API.

---

### Q137. Standalone vs distributed mode?
**Answer:** Standalone: a single process with offsets in a local file; used for development or single-agent cases. Distributed: a cluster of workers sharing the load; connector configs, offsets, and status are stored in internal Kafka topics; tasks are rebalanced automatically on worker failure. Use distributed in production.

---

### Q138. Explain connector, task, worker, converter, and transform.
**Answer:** A **worker** is a Connect JVM process. A **connector** is the logical job; it creates **tasks** (the units that actually move data, parallelized by `tasks.max`). **Converters** (Avro, JSON, String) translate between Connect's internal data and Kafka bytes. **Transforms (SMTs)** apply lightweight per-record changes (rename, mask, route, flatten).

---

### Q139. How does the JDBC source connector pull data, and what are its limitations?
**Answer:** Modes: `bulk` (full table each poll), `incrementing` (new rows by an increasing ID), `timestamp` (updated rows by timestamp), and `timestamp+incrementing` (most reliable for inserts and updates). Limitations: no deletes are captured, long transactions can cause missed rows if timestamps are committed out of order, polling puts load on the database, and there is no true change history. For fuller fidelity use log-based CDC.

---

### Q140. What is CDC and how does Debezium work?
**Answer:** Change Data Capture streams row-level changes from a database's transaction log: MySQL binlog, PostgreSQL WAL via logical decoding, SQL Server CDC tables, Oracle LogMiner. Debezium (a Kafka Connect source) takes an initial snapshot, then reads the log and emits an event per change to a topic like `server.schema.table`, with an envelope containing `before`, `after`, `op` (`c` create, `u` update, `d` delete, `r` snapshot read), `ts_ms`, and `source` metadata (log position). It captures deletes and preserves ordering per row key.

---

### Q141. Show a Debezium MySQL connector configuration.
**Answer:**
```json
{
  "name": "mysql-orders-cdc",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "mysql.internal",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "${file:/secrets/db.properties:password}",
    "database.server.id": "184054",
    "topic.prefix": "shop",
    "table.include.list": "sales.orders,sales.customers",
    "schema.history.internal.kafka.bootstrap.servers": "b1:9092",
    "schema.history.internal.kafka.topic": "schema-history.shop",
    "snapshot.mode": "initial",
    "tasks.max": "1",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.delete.handling.mode": "rewrite"
  }
}
```
Property names are for Debezium 2.x; check the documentation of your version. Debezium MySQL runs with a single task because it reads one binlog stream. Use config providers rather than plaintext passwords.

---

### Q142. Scenario: Debezium on PostgreSQL causes the database disk to fill up. Why?
**Answer:** A logical replication slot retains WAL until the consumer (Debezium) confirms it. If the connector is stopped, lagging, or the slot is orphaned, WAL accumulates and can fill the disk. Prevention: monitor `pg_replication_slots` lag, alert on connector downtime, set `max_slot_wal_keep_size`, use heartbeat events for low-traffic tables, and drop unused slots.

---

### Q143. Configure a sink connector that writes Kafka data to S3 as partitioned Parquet.
**Answer:**
```json
{
  "name": "s3-orders-sink",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "topics": "orders",
    "s3.bucket.name": "datalake-raw",
    "s3.region": "ap-south-1",
    "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
    "storage.class": "io.confluent.connect.s3.storage.S3Storage",
    "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
    "path.format": "'dt'=YYYY-MM-dd/'hr'=HH",
    "partition.duration.ms": "3600000",
    "timestamp.extractor": "RecordField",
    "timestamp.field": "event_time",
    "locale": "en-IN",
    "timezone": "UTC",
    "flush.size": "100000",
    "rotate.interval.ms": "600000",
    "tasks.max": "4"
  }
}
```
`flush.size` and rotation settings control file sizes (avoid thousands of tiny files); using event time for the path groups late data correctly, but needs the right timezone handling.

---

### Q144. How does Connect handle errors and dead letters?
**Answer:** `errors.tolerance=none` (default) fails the task on the first bad record; `errors.tolerance=all` skips bad records. For sink connectors, `errors.deadletterqueue.topic.name` sends failed records (with error headers via `errors.deadletterqueue.context.headers.enable=true`) to a DLQ topic. Also `errors.retry.timeout` and `errors.log.enable`. Source connectors do not support a DLQ in the same way.

---

### Q145. Name some common Single Message Transforms (SMTs).
**Answer:** `ExtractField`, `ReplaceField` (rename/drop/mask), `MaskField`, `InsertField` (add static or metadata), `TimestampRouter` and `RegexRouter` (rewrite topic names), `Flatten`, `TimestampConverter`, `ValueToKey`, and Debezium's `ExtractNewRecordState`. SMTs are for light, stateless changes; heavy logic belongs in Kafka Streams/Flink/Spark.

---

### Q146. Which Connect REST API calls do you use?
**Answer:**
```bash
curl -s localhost:8083/connectors
curl -s -X POST -H "Content-Type: application/json" --data @conn.json localhost:8083/connectors
curl -s localhost:8083/connectors/s3-orders-sink/status
curl -s -X PUT  localhost:8083/connectors/s3-orders-sink/pause
curl -s -X PUT  localhost:8083/connectors/s3-orders-sink/resume
curl -s -X POST "localhost:8083/connectors/s3-orders-sink/restart?includeTasks=true&onlyFailed=true"
curl -s -X DELETE localhost:8083/connectors/s3-orders-sink
```

---

### Q147. What internal topics does distributed Connect use?
**Answer:** `config.storage.topic` (connector configs), `offset.storage.topic` (source connector offsets), and `status.storage.topic` (connector and task status). They should be compacted, with replication factor 3 and the right partition counts (config: 1 partition).

---

### Q148. How do you scale a Connect pipeline?
**Answer:** Add workers to the cluster (tasks rebalance across them), raise `tasks.max` up to the useful parallelism (sink: number of topic partitions; JDBC source: number of tables), tune batch sizes (`batch.size`, `consumer.override.max.poll.records`), and make sure the target system handles the throughput. Some connectors (Debezium) are limited to one task by design.

---

### Q149. Scenario: build a pipeline MySQL → Kafka → Snowflake/warehouse. What are the main design points?
**Answer:**
1. Debezium MySQL source with an initial snapshot, then binlog streaming; one topic per table, Avro with Schema Registry.
2. Unwrap the envelope (`ExtractNewRecordState`) or keep `before/after` for auditing; keep delete markers.
3. Sink connector (Snowflake/JDBC/S3 then COPY) in upsert mode keyed by primary key; or land raw events and build current-state tables with `MERGE` using the latest event per key.
4. Handle schema changes: Debezium tracks DDL; schema compatibility rules; the sink must support evolution.
5. Operations: monitor connector status, lag, snapshot duration, binlog retention on the source; a replay and reconciliation process (row counts and checksums vs source).

---

### Q150. Scenario: a sink connector task is FAILED and the pipeline is stuck. How do you troubleshoot?
**Answer:** Check `GET /connectors/<name>/status` for the stack trace in the task `trace` field and the worker logs. Typical causes: deserialization error (schema or converter mismatch), bad record in the sink (constraint violation, type mismatch), target unreachable or throttled, or authorization changes. Fix the root cause, then restart the task (`restart?includeTasks=true`). Use `errors.tolerance=all` with a DLQ only if skipping bad records is acceptable, and replay the DLQ later. Reset or skip offsets only as a last resort and with an audit.

---

# SECTION 8: KAFKA STREAMS, KSQLDB, SPARK & FLINK WITH KAFKA

### Q151. What is Kafka Streams and how does it differ from Spark or Flink?
**Answer:** Kafka Streams is a Java library (not a cluster) for building stream processing applications that read from and write to Kafka. You deploy it like any normal application and scale by running more instances; Kafka handles partitioning, fault tolerance, and state. Spark and Flink are separate processing clusters with broader connectors and richer APIs (SQL, ML, batch). Streams is excellent for Kafka-to-Kafka microservice-style processing; Spark/Flink fit heavy analytics and multi-source jobs.

---

### Q152. Explain KStream, KTable, and GlobalKTable.
**Answer:** `KStream`: an unbounded stream of independent events (every record is an insert). `KTable`: a changelog view where each key holds its latest value (an update stream; backed by a compacted topic and a local state store). `GlobalKTable`: a table fully replicated to every instance (no co-partitioning needed), good for small reference data.

---

### Q153. Stateless vs stateful operations in Streams?
**Answer:** Stateless: `filter`, `map`, `mapValues`, `flatMap`, `branch`/`split`, `selectKey`; no memory of earlier records. Stateful: `count`, `aggregate`, `reduce`, joins, and windowing; they need local state stores backed by changelog topics. Stateful operations require co-partitioning and careful capacity for state size.

---

### Q154. What window types exist?
**Answer:** **Tumbling** (fixed, non-overlapping, e.g., every 5 minutes), **hopping** (fixed size, overlapping, advance < size), **sliding** (based on time difference between records), and **session** (dynamic windows closed by an inactivity gap). Windows rely on event time, with a **grace period** for late records.

---

### Q155. Write a simple Kafka Streams aggregation: orders per region per minute.
**Answer:**
```java
StreamsBuilder builder = new StreamsBuilder();
KStream<String, Order> orders = builder.stream("orders",
    Consumed.with(Serdes.String(), orderSerde));

orders.groupBy((key, order) -> order.getRegion(), Grouped.with(Serdes.String(), orderSerde))
      .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ofSeconds(30)))
      .count(Materialized.as("orders-per-region-store"))
      .toStream()
      .map((windowedKey, count) -> KeyValue.pair(windowedKey.key() + "@" + windowedKey.window().start(), count))
      .to("orders-per-minute", Produced.with(Serdes.String(), Serdes.Long()));

Properties props = new Properties();
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-agg");
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
new KafkaStreams(builder.build(), props).start();
```
`groupBy` on a different key triggers a repartition topic automatically.

---

### Q156. How does Kafka Streams manage state and recover from failures?
**Answer:** Each task keeps state in a local store (RocksDB by default) and logs every update to a compacted **changelog topic**. If an instance fails, another instance restores the store by replaying the changelog. `num.standby.replicas` keeps warm copies to speed up failover. Large state means long restores, so use standbys and persistent volumes.

---

### Q157. What is co-partitioning and why do joins need it?
**Answer:** For a join, records with the same key must be handled by the same task, which requires both input topics to have the same number of partitions and the same partitioning strategy (same key and partitioner). Otherwise results are silently incorrect. Fix by repartitioning one side (`repartition`/`through`) or using a `GlobalKTable`.

---

### Q158. What join types are available in Kafka Streams?
**Answer:** Stream-stream (windowed, inner/left/outer), stream-table (enrich events with the latest table value; not windowed), table-table (inner/left/outer), stream-GlobalKTable (no co-partitioning), and table-table foreign-key joins. Stream-stream joins require a time window and keep both sides in state for that duration.

---

### Q159. How do late events work, and what is `suppress`?
**Answer:** A window accepts out-of-order records until the stream time passes `window end + grace`. Records after that are dropped (and counted in metrics). By default Streams emits updated results continuously; `suppress(Suppressed.untilWindowCloses(...))` emits only the final result per window, trading latency for a single final output.

---

### Q160. How do you scale a Kafka Streams application?
**Answer:** The maximum parallelism equals the number of input partitions (one task per partition per sub-topology). Scale by running more instances with the same `application.id` or by increasing `num.stream.threads`; Kafka rebalances tasks automatically. Beyond the partition count, add partitions (watch state and key mapping) or restructure the topology.

---

### Q161. What does `exactly_once_v2` do in Streams?
**Answer:** It wraps the read-process-write cycle (including state store changelog updates and offset commits) in Kafka transactions per task thread, so the output topics, state, and offsets commit atomically. It requires brokers 2.5+, uses fewer producers than v1, and slightly increases latency (commit interval defaults to 100 ms when EOS is on).

---

### Q162. What are interactive queries?
**Answer:** Querying the local state stores of a Kafka Streams application from outside (REST endpoint), for example to look up the latest count by key. Because state is partitioned across instances, the app must route a query to the instance that holds the key (via `queryMetadataForKey`).

---

### Q163. What is ksqlDB?
**Answer:** A streaming SQL engine on top of Kafka Streams. You define streams and tables with SQL and run continuous queries that write results to topics, without writing Java code. It suits quick prototyping, filtering, enrichment, and simple aggregations; complex logic or custom libraries are better in Streams/Flink.

---

### Q164. Give a ksqlDB example.
**Answer:**
```sql
CREATE STREAM orders (order_id VARCHAR KEY, region VARCHAR, amount DOUBLE, ts BIGINT)
  WITH (KAFKA_TOPIC='orders', VALUE_FORMAT='JSON', TIMESTAMP='ts');

CREATE TABLE sales_per_region AS
  SELECT region, COUNT(*) AS orders, SUM(amount) AS revenue
  FROM orders
  WINDOW TUMBLING (SIZE 1 MINUTE)
  GROUP BY region;

SELECT * FROM sales_per_region EMIT CHANGES;     -- push query
```

---

### Q165. Kafka Streams vs Flink vs Spark Structured Streaming: how do you choose?
**Answer:**
| | Kafka Streams | Flink | Spark Structured Streaming |
|---|---|---|---|
| Deployment | Library in your app | Cluster | Cluster |
| Latency | Low (ms) | Very low (ms), true streaming | Micro-batch (sub-second to seconds); continuous mode limited |
| Sources/sinks | Kafka-centric | Many | Many (lake, JDBC, files) |
| State | RocksDB + changelog | Managed, large state, savepoints | State store with checkpoints |
| Best for | Microservices on Kafka | Complex event processing, large stateful jobs | Unified batch + stream, lakehouse ETL, ML |

---

### Q166. Read from Kafka with Spark Structured Streaming and parse JSON.
**Answer:**
```python
from pyspark.sql import functions as F, types as T

schema = T.StructType([
    T.StructField("order_id", T.StringType()),
    T.StructField("region", T.StringType()),
    T.StructField("amount", T.DoubleType()),
    T.StructField("event_time", T.TimestampType()),
])

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "b1:9092")
       .option("subscribe", "orders")
       .option("startingOffsets", "latest")
       .option("maxOffsetsPerTrigger", 200000)
       .option("failOnDataLoss", "false")
       .load())

orders = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("o"),
                     "topic", "partition", "offset")
             .select("o.*", "topic", "partition", "offset"))

agg = (orders.withWatermark("event_time", "10 minutes")
             .groupBy(F.window("event_time", "5 minutes"), "region")
             .agg(F.sum("amount").alias("revenue")))
```
The Kafka source gives columns `key`, `value` (binary), `topic`, `partition`, `offset`, `timestamp`, `timestampType`.

---

### Q167. How does Spark track Kafka offsets and recover?
**Answer:** Spark stores consumed offsets in its own **checkpoint** directory, not in Kafka's consumer group commits, so after a restart it resumes exactly from the checkpoint (`startingOffsets` applies only to the first run). Never delete or reuse a checkpoint path between different queries. `failOnDataLoss=true` fails the job if offsets it needs have been deleted by retention; `false` skips and continues (data loss risk).

---

### Q168. Write streaming data from Spark to a Delta/Iceberg table idempotently.
**Answer:**
```python
def upsert(batch_df, batch_id):
    batch_df.createOrReplaceTempView("updates")
    batch_df.sparkSession.sql("""
        MERGE INTO lake.orders t
        USING (SELECT * FROM (
                 SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY event_time DESC) rn
                 FROM updates) WHERE rn = 1) s
        ON t.order_id = s.order_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""")

(orders.writeStream.foreachBatch(upsert)
       .option("checkpointLocation", "s3://lake/chk/orders")
       .trigger(processingTime="1 minute").start())
```
Deduplicating inside the batch and using `MERGE` makes retries safe.

---

### Q169. How does Flink achieve exactly-once with Kafka?
**Answer:** Flink takes periodic distributed checkpoints (barriers flowing through the job) that snapshot operator state and Kafka source offsets. On failure it restores the latest checkpoint and rewinds the sources. With a transactional Kafka sink, writes are committed only when the checkpoint completes (two-phase commit), giving end-to-end exactly-once for Kafka-to-Kafka. Event-time processing uses watermarks to handle late data.

---

### Q170. Scenario: real-time "orders per minute per region" dashboard with late events. Design it.
**Answer:** Orders topic keyed by `order_id` with `event_time` in the payload → stream job (Flink/Spark/Streams) with event-time tumbling windows of 1 minute, a watermark (e.g., 2 minutes) or grace period for late data, and the aggregated result written to a serving store (Druid/ClickHouse/Elasticsearch/Redis) keyed by `(region, window_start)` using upserts, so later corrections overwrite earlier values. Very late events are routed to a side output and corrected in a batch reconciliation. Monitor watermark lag, dropped-late-record counts, and consumer lag.

---

# SECTION 9: OPERATIONS, SECURITY, MONITORING & PERFORMANCE

### Q171. Which broker metrics do you monitor and alert on?
**Answer:**
- `UnderReplicatedPartitions` and `UnderMinIsrPartitionCount` (should be 0).
- `OfflinePartitionsCount` (must be 0) and `ActiveControllerCount` (exactly 1 across the cluster).
- `IsrShrinksPerSec` / `IsrExpandsPerSec` (flapping indicates trouble).
- `RequestHandlerAvgIdlePercent` and `NetworkProcessorAvgIdlePercent` (low values mean saturation).
- Request latency (`TotalTimeMs` for Produce/Fetch), `BytesIn/OutPerSec`.
- Disk usage and I/O wait, JVM GC pauses, open file descriptors, network errors.

---

### Q172. How do you monitor consumer lag in production?
**Answer:** Export per-partition lag with Burrow, `kafka-lag-exporter`, or the cloud provider's metrics, scrape with Prometheus, and dashboard in Grafana. Alert on lag trending upward for N minutes and on time-based lag (how many minutes behind), not on a fixed number alone, since healthy lag varies by topic throughput. Track alongside consumer group state (rebalancing, empty).

---

### Q173. How do you secure a Kafka cluster?
**Answer:** **Encryption in transit**: TLS between clients and brokers and between brokers. **Authentication**: mTLS, SASL (SCRAM-SHA-256/512, GSSAPI/Kerberos, OAUTHBEARER, PLAIN only over TLS). **Authorization**: ACLs (or Ranger/RBAC in managed platforms). **Governance**: separate users per application, quotas, audit logs, secrets in a vault, network isolation (private subnets, security groups), and encryption at rest at the disk/volume level.

---

### Q174. Show client configuration for SASL_SSL with SCRAM.
**Answer:**
```properties
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="etl-app" password="${SECRET}";
ssl.truststore.location=/etc/kafka/truststore.jks
ssl.truststore.password=${TRUSTSTORE_PASSWORD}
```
For librdkafka clients (Python/Go): `security.protocol`, `sasl.mechanisms`, `sasl.username`, `sasl.password`, and `ssl.ca.location`.

---

### Q175. How do you grant access with ACLs?
**Answer:**
```bash
# consumer: read the topic and use the group
kafka-acls.sh --bootstrap-server b1:9094 --command-config admin.properties --add \
  --allow-principal User:etl-app --operation Read --operation Describe --topic orders
kafka-acls.sh --bootstrap-server b1:9094 --command-config admin.properties --add \
  --allow-principal User:etl-app --operation Read --group orders-etl
# producer: write to the topic
kafka-acls.sh --bootstrap-server b1:9094 --command-config admin.properties --add \
  --allow-principal User:checkout-svc --operation Write --operation Describe --topic orders
```
Prefer prefixed ACLs (`--resource-pattern-type prefixed --topic payments.`) for team namespaces, and set default deny.

---

### Q176. What about encryption at rest and sensitive fields?
**Answer:** Kafka doesn't encrypt log files itself; use encrypted volumes (EBS/LUKS) or cloud-managed storage encryption. For sensitive fields (PII, card data), apply application-level or field-level encryption/tokenization before producing (or with an SMT/connector), so even people with topic access can't read raw values, and manage keys in a KMS.

---

### Q177. How do you replicate data across clusters or regions?
**Answer:** **MirrorMaker 2** (a Connect-based tool) replicates topics, ACLs, configs, and consumer group offsets (checkpoints) between clusters, asynchronously; topic names are prefixed with the source cluster alias by default. Confluent Cluster Linking and similar vendor tools offer offset-preserving mirroring. A stretch cluster across zones works with low latency links and rack awareness, but not across distant regions because replication latency hurts producers using `acks=all`.

---

### Q178. How do you perform a rolling upgrade?
**Answer:** Read the release notes, back up configs, then upgrade brokers one at a time: shut down gracefully (controlled shutdown moves leadership), upgrade the binaries, restart, and wait for the broker to rejoin the ISR for all partitions (zero under-replicated partitions) before touching the next one. Finalize the new `metadata.version` (`kafka-features.sh upgrade`) or `inter.broker.protocol.version` only after all brokers run the new code, since that step is hard to roll back. Upgrade clients after the brokers.

---

### Q179. How do you add brokers and rebalance data?
**Answer:** New brokers join empty; existing partitions don't move automatically. Use `kafka-reassign-partitions.sh` (`--generate`, `--execute`, `--verify`) with a **replication throttle** (`--throttle 50000000`) so migration doesn't starve production traffic, or use **Cruise Control** to compute and execute balanced moves based on load. Afterwards run preferred leader election to spread leadership.

---

### Q180. Which JVM and OS settings matter for brokers?
**Answer:** A moderate heap (about 6 GB; Kafka relies on the OS page cache, so leave most RAM free), G1GC, high file descriptor limits (100k+), `vm.max_map_count` raised for many segments, `vm.swappiness=1`, XFS or ext4 with `noatime`, dedicated disks for logs (JBOD or RAID10), and fast, sufficient network (10 GbE+). Avoid network filesystems for log dirs.

---

### Q181. How do you do capacity planning? Example.
**Answer:** Storage = ingress rate × retention × replication factor ÷ compression, plus headroom. Example: 50 MB/s ingress × 86,400 s = 4.32 TB/day; × 7 days = 30.2 TB; × RF 3 = about 91 TB raw; add 30–40% headroom, so plan roughly 120 TB (less if compression gives 2–3x). Network: inbound 50 MB/s plus follower replication traffic of about 100 MB/s inside the cluster, plus consumer egress equal to 50 MB/s × the number of consumer groups. Also size partitions per broker (a few thousand) and CPU for TLS and compression.

---

### Q182. What is tiered storage?
**Answer:** A feature (KIP-405, generally available from Kafka 3.9) that offloads older log segments to cheap remote storage (like S3) while keeping recent data on local broker disks. It allows long retention without adding brokers solely for disk, faster broker recovery (less local data to replicate), and lower cost. Reads of old data go through the remote tier and are slower.

---

### Q183. Scenario: a broker's disk is almost full. What do you do?
**Answer:** Don't delete segment files manually. Identify the largest topics/partitions (`kafka-log-dirs.sh --describe`), temporarily reduce `retention.ms`/`retention.bytes` on non-critical large topics so segments are deleted, check for a runaway producer or a stuck compaction (`log-cleaner`), then add disk or brokers and reassign partitions. If a log directory fills completely, the broker can go offline for that directory, so alert early (70–80%), and enforce topic retention standards.

---

### Q184. What causes under-replicated partitions?
**Answer:** A broker is down or restarting, a slow broker (disk I/O saturation, long GC pauses, CPU), network problems between brokers, overloaded followers because of too many partitions or heavy traffic, throttled reassignments, or insufficient fetcher threads. Investigate which broker appears in the missing ISRs for most partitions, then look at its logs, disk, and GC.

---

### Q185. Scenario: the ISR keeps shrinking and expanding (flapping). What is going on?
**Answer:** Followers repeatedly miss the lag threshold (`replica.lag.time.max.ms`). Typical causes: bursts of traffic exceeding replication capacity, disk latency, long GC pauses, network jitter or packet loss between AZs, and CPU saturation from compression/TLS. Check the broker's request latency, disk metrics, GC logs, and network. Fix the bottleneck (more brokers, faster disks, bigger fetch settings, JVM tuning) rather than increasing the lag threshold blindly.

---

### Q186. Options for running Kafka in the cloud or on Kubernetes?
**Answer:** Managed: Amazon MSK, Confluent Cloud, Azure Event Hubs (Kafka-compatible endpoint), Aiven, and others. They reduce operational work (patching, scaling, monitoring) at higher cost and with some feature limits. Self-managed on Kubernetes: operators like Strimzi automate brokers, topics (`KafkaTopic` CRs), users, and upgrades, but require careful storage (persistent volumes with good IOPS), rack awareness, and resource tuning. Compatible alternatives (Redpanda) also exist.

---

### Q187. How do you govern topics: naming, ownership, and creation?
**Answer:** Use a convention like `<domain>.<entity>.<event>.<version>` (for example `payments.order.paid.v1`), disable `auto.create.topics.enable`, create topics via infrastructure as code (Terraform, Strimzi CRs, GitOps) with required settings (partitions, RF=3, `min.insync.replicas=2`, retention), tag owners, enforce ACLs by prefix, set quotas per team, and review/retire unused topics regularly.

---

### Q188. CLI tools worth knowing besides `kafka-topics`?
**Answer:**
```bash
kafka-configs.sh --describe --entity-type topics --entity-name orders      # view overrides
kafka-log-dirs.sh --bootstrap-server b1:9092 --describe --topic-list orders # sizes per partition
kafka-dump-log.sh --files /data/kafka/orders-0/00000000000000000000.log --print-data-log
kafka-get-offsets.sh --bootstrap-server b1:9092 --topic orders --time -1    # log end offsets
kafka-leader-election.sh --bootstrap-server b1:9092 --election-type preferred --all-topic-partitions
kafka-delete-records.sh --bootstrap-server b1:9092 --offset-json-file del.json
kafka-metadata-quorum.sh --bootstrap-server b1:9092 describe --status       # KRaft health
kafka-producer-perf-test.sh / kafka-consumer-perf-test.sh                    # load tests
```

---

# SECTION 10: REAL INDUSTRY SCENARIOS

### Q189. Scenario: design a real-time clickstream pipeline for an e-commerce site.
**Answer:**
1. **Collect**: web/app SDKs send events to a collector service that produces to `clicks.raw` (Avro/Protobuf, key = session or user ID, 48 partitions, `acks=all`, idempotent, lz4/zstd, retention 7 days).
2. **Process**: a Flink/Spark/Streams job filters bots, sessionizes (30-minute gap, event time with watermarks), enriches with user/product data (compacted reference topic or table), and writes `clicks.enriched`; bad records go to a DLQ.
3. **Serve**: sink connectors write to S3/Iceberg for analytics and to ClickHouse/Druid/Elasticsearch for live dashboards; a feature store consumes aggregates for recommendations.
4. **Governance**: Schema Registry, PII hashing, ACLs, quotas.
5. **Operations**: lag and throughput alerts, replay capability from the raw topic or lake, data quality checks (null rate, event-time skew).

---

### Q190. Scenario: ingest telemetry from 1 million IoT devices sending a message every 10 seconds.
**Answer:** That's about 100,000 msgs/s; at 500 bytes it's roughly 50 MB/s. Devices connect to an MQTT broker (or an HTTP gateway), bridged into Kafka via a Connect MQTT source. Key by `device_id` (keeps per-device order), use 48–96 partitions, compression, and RF=3. Keep a raw topic (3–7 days) and a compacted `device-state` topic (latest reading per device). Stream jobs compute rolling aggregates with event-time windows and grace periods for devices that reconnect after being offline. Long-term history lands in the lake via the S3/Iceberg sink; tiered storage reduces broker disk cost.

---

### Q191. Scenario: traffic will spike 10x on a sale day. How do you prepare Kafka?
**Answer:** Load-test with `kafka-producer-perf-test` and replay-like traffic well before the event. Ensure partitions give headroom (adding partitions under load changes key mapping) and that consumers can scale to the partition count; pre-scale brokers, disks, and network; set producer batching/compression and quotas so noisy clients can't starve others; enable autoscaling for consumers based on lag (KEDA); confirm retention and disk capacity cover the peak plus catch-up; add backpressure and circuit breakers on downstream systems; and prepare a runbook with dashboards, alert thresholds, and rollback steps.

---

### Q192. Scenario: the downstream database was down for 3 hours. The consumer lag is now huge. How do you recover?
**Answer:** First, confirm Kafka retention exceeds the outage plus catch-up time, or data will be deleted before it's read. After the database recovers, don't let consumers hammer it at full speed: throttle with pause/resume or rate limits, scale consumers up to the partition count, use batched/bulk writes, and increase `max.poll.records`. Prioritize what matters (for non-critical dashboards, you might skip to recent data and backfill later). Keep the sink idempotent because retries will have caused partial writes. Monitor time-lag falling and DB load, and add alerts on lag and on consumer failure so the outage is noticed sooner.

---

### Q193. Scenario: migrate from RabbitMQ to Kafka. What's your plan?
**Answer:**
1. Map exchanges/queues to topics and define keys and partition counts; remember semantics differ (offsets instead of per-message acks, no priority queues or per-message TTL, routing logic moves into consumers/streams).
2. Bridge old and new during transition (RabbitMQ source connector, or dual-publish from the producer).
3. Migrate consumers one at a time, with idempotent handling; design retry and dead letter topics to replace queue-based retry/DLX features.
4. Run in parallel and compare outputs (counts, checksums), then cut over producers and decommission queues.
5. Update monitoring, runbooks, and team skills (lag, rebalances, retention).

---

### Q194. Scenario: a bug in a stream job corrupted output for the last 2 days. How do you reprocess?
**Answer:** Fix and test the code, then run the corrected job against the historical range as a new consumer group (`group.id` or a new Streams `application.id`) starting from the timestamp before the bug (`--reset-offsets --to-datetime`, or Spark's `startingTimestamp`), writing to a new target/version of the table or topic, not over live data. Validate counts and key metrics against the source, then swap consumers/readers to the corrected output (or `MERGE` corrections into the original). Ensure Kafka retention still covers the range; if not, replay from raw data stored in the lake. Keep sinks idempotent and throttle the backfill so it doesn't disrupt real-time processing.

---

### Q195. Scenario: the primary region fails. Describe your Kafka disaster recovery approach.
**Answer:** Run an active-passive (or active-active) pair of clusters with MirrorMaker 2 replicating topics, configs, and consumer group offsets. On failure, switch producers to the DR cluster (DNS/config), start consumers there using translated offsets (`sync.group.offsets.enabled=true`), and accept that replication is asynchronous so the RPO is greater than zero: the last few seconds of data may be missing, and some records may be reprocessed, so consumers must be idempotent. Define RTO/RPO targets, automate failover steps, and rehearse regularly; plan the failback (reverse replication and reconcile).

---

### Q196. Scenario: producers report high latency and timeouts on a cluster that was fine yesterday. How do you troubleshoot?
**Answer:**
1. Check broker health first: under-replicated or offline partitions, active controller count, ISR flapping.
2. Look at request latency breakdown (queue, local, remote, response) to see whether the bottleneck is disk, replication, or network threads; check `RequestHandlerAvgIdlePercent`.
3. Check system resources: disk I/O wait, disk usage, CPU, GC pauses, network saturation or packet loss.
4. Look for a noisy client or new workload (a big new producer, many small requests, a consumer re-reading from the start) and apply quotas.
5. Check leader imbalance (run preferred leader election) and hot partitions.
6. Review recent changes: config, deployments, partition reassignments (are throttles set?), TLS/cert changes.
Fix the bottleneck, then add alerts that would have caught it earlier.

---

### Q197. Scenario: design a real-time card fraud detection pipeline on Kafka.
**Answer:** Card transactions go to `transactions` keyed by `card_id`. A Flink or Kafka Streams job keeps per-card state (counts and amounts over the last 1/5/60 minutes, last location for geo-velocity checks), joins with a compacted `customer-risk` table, applies rules and an ML model score, and emits `fraud.alerts` plus an enriched decision topic. Use event-time windows with small grace periods, exactly-once processing (`exactly_once_v2`/Flink checkpoints), standby replicas for fast failover, and latency SLOs (for example, p99 < 200 ms). Alerts feed case management and card-blocking services; all inputs and decisions are retained for audit and model retraining/replay. Provide a fallback (default allow/deny rules) if the stream job is unavailable.

---

### Q198. Scenario: a user requests deletion of their personal data (GDPR/DPDP). How do you handle it in Kafka?
**Answer:** Kafka's log is immutable, so design for it upfront: keep PII out of events where possible (use surrogate IDs), use short retention for topics with personal data, and for compacted topics write a tombstone for the user's key. For long-retention or multi-event topics, use **crypto-shredding**: encrypt PII fields with a per-user key stored in a KMS, and delete the key on request so the data in Kafka (and backups) becomes unreadable. Also propagate deletions to downstream systems (lake, warehouse, caches) through a deletion event stream, and keep an audit trail.

---

### Q199. Scenario: the Kafka bill has doubled. How do you reduce cost without hurting reliability?
**Answer:** Measure per-topic and per-team usage first. Then: enable `zstd`/`lz4` compression and larger batches; trim retention to what the business needs and use tiered storage for long retention; remove unused topics and consumer groups; reduce partition counts where over-provisioned; use follower fetching (`client.rack`) to cut cross-AZ network charges; keep RF=3 only for critical topics (RF=2 for easily reproducible data, with a clear risk decision); right-size brokers and disks; consolidate tiny topics; and charge back usage to teams to drive good behavior.

---

### Q200. "Tell me about a Kafka project you worked on." How do you answer?
**Answer:** Use a structured story:
- **Context**: business problem, event sources, volume (e.g., 20k msgs/s, 100 GB/day), latency and reliability needs.
- **Design**: topics and keys, partition counts and why, replication settings (RF=3, `min.insync.replicas=2`, `acks=all`), schemas (Avro + Schema Registry), consumers/processing (Spark/Flink/Streams/Connect), sinks.
- **Your role**: pieces you built (producer library, consumer with DLQ and retries, Debezium CDC, lag dashboards).
- **Challenges and results**: a real problem (rebalance storms, lag after outages, duplicates, hot partitions, schema breaks) with the fix and measurable outcome (lag from 40 min to under 1 min, duplicates eliminated by idempotent upserts).
- **Operations**: monitoring, alerts, replay/backfill process, security, upgrades.
- **Learnings**: what you'd improve (data contracts, cooperative rebalancing, tiered storage, automated reconciliation).

Be ready to discuss trade-offs: delivery semantics, ordering vs parallelism, and batch vs streaming.

---

