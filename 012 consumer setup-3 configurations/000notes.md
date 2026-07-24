# Consumer Setup — Part 3 (Other Configurations)

## Step 1 — Consumer starts and wants to join a Group

```
Doubt: Can a consumer join more than 1 group?
Answer: No — 1 consumer can join only 1 group.
```

## Step 2 — Find the partition of `_consumer_offsets`

```
hash(group.id) % N  →  gives the partition number of internal topic
                       "_consumer_offsets" that stores this group's offsets
```

## Step 3 — Consumer requests metadata (first time or refresh)

```
Consumer invokes one of the bootstrap servers (any broker), asks for
metadata, and caches it.

Doubt: How frequently is this metadata refreshed?
Answer: Multiple triggers cause a refresh:
  - Periodic refresh (default 5 minutes):
      spring.kafka.consumer.properties.metadata.max.age.ms=300000
  - On-demand refresh:
      - Broker returns NOT_LEADER_FOR_PARTITION
      - Information missing from cache
      - etc.
```

## Step 4 — Find Group Coordinator, send JoinGroupRequest

```
Request contains:
  - Consumer Group it needs to join
  - Topics it wants to subscribe to

Doubt 1: Can 1 consumer request to join more than 1 topic?
Doubt 2: Can multiple consumers in a group request to join different topics?
Doubt 3: Can multiple consumers in a group request to join the same topic?

→ All covered in Part-2 of Consumer Setup.
```

## Step 5 — Partition Assignment

```
Broker3 (Group Coordinator)
  Group: order-consumer-group
  Topic: order-events
    Consumer1: handle Partition-0
    Consumer2: handle Partition-1
    Consumer3: handle Partition-2

Doubt: How are partitions divided among consumers in the group?
```

### Partition Assignment Strategies

```
1. Range Assignor (Default)
2. Round Robin
3. Sticky Assignor
4. Cooperative Sticky
```

**Range Assignor (Default):**

```
- Lays out Partitions in numeric order
- Lays out Consumers in alphabetical order
- Logic: partitions / consumers = 6 / 3 = 2 each

Consumers: C1, C2, C3
Topic: order-events, Partitions: P0..P5

  C1: P0, P1
  C2: P2, P3
  C3: P4, P5
```

**Round Robin:**

```
- Just distributes partitions 1-by-1 to all consumers

  C1: P0, P3
  C2: P1, P4
  C3: P2, P5
```

**Sticky Assignor:**

```
- Starts with a Round Robin-like assignment
  C1: P0, P3
  C2: P1, P4
  C3: P2, P5

- Its real advantage shows up during REBALANCE (when a consumer is
  added or removed from the group).
```

**Cooperative Sticky:**

```
Same starting point, but a fundamentally different rebalance PROTOCOL
(see below).
```

### What happens during a Rebalance (say C3 crashes)

Starting point: `C1: P0,P3` / `C2: P1,P4` / `C3: P2,P5`

**Range Assignor / Round Robin (both are "Eager" rebalancers):**

```
- Every remaining consumer (C1, C2) DROPS the partitions they were
  holding.
- Every consumer STOPS reading.
- Algorithm runs a FULL recalculation from scratch for the remaining
  2 consumers.

Example result:
  C1: P0, P2, P4
  C2: P1, P3, P5

Problem: C1 previously had P0 and P3. After rebalance, P3 is removed
and P2, P4 are added. So C1 has to do a full cleanup — clear cache for
P3, and set up fresh state/offsets for P2 and P4. Lots of churn.
```

**Sticky Assignor:**

```
- Smarter: during rebalance, tries to achieve even distribution (as
  much as possible) WHILE keeping consumers attached to the partitions
  they already had.

Example: C3 crashes → its partitions (P2, P5) get redistributed evenly
to C1 and C2, but C1 and C2 KEEP the partitions they already had:

  C1: P0, P3, P2   (kept P0,P3; gained P2)
  C2: P1, P4, P5   (kept P1,P4; gained P5)

Much less churn than Range/Round Robin.
```

**All 3 above (Range, Round Robin, Sticky) are "Eager" rebalancers:**

```
When rebalance happens, ALL consumers stop reading from their
partitions until new assignments are finalized.
```

**Cooperative (protocol used by Cooperative Sticky Assignor):**

```
Step1: Algorithm decides which OLD partitions each consumer will still
       keep.
Step2: Consumers CONTINUE reading from the partitions they keep — only
       partitions being removed from them (or newly added to them) get
       updated later.

→ No full stop-the-world pause; only the affected partitions are
  paused/reassigned, not all of them.
```

### Configuring the assignment strategy

```properties
spring.kafka.consumer.properties.partition.assignment.strategy=org.apache.kafka.clients.consumer.RangeAssignor
# or
spring.kafka.consumer.properties.partition.assignment.strategy=org.apache.kafka.clients.consumer.RoundRobinAssignor
# or
spring.kafka.consumer.properties.partition.assignment.strategy=org.apache.kafka.clients.consumer.StickyAssignor
# or
spring.kafka.consumer.properties.partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

## Step 6 & 7 — Fetching the last committed offset

```
Consumer receives partition assignment (order-events, P0)
Consumer asks Group Coordinator: "till where has the offset been read
for P0 of topic order-events?"

Doubt: What if there's no offset info present?

Group Coordinator fetches offset info from the internal
__consumer_offsets topic:

  Offset exists in __consumer_offsets?
    YES → Fetch the saved offset and return it
    NO  → Look at config: spring.kafka.consumer.auto-offset-reset
```

```
auto-offset-reset options:

  "latest" (default) → Consumer reads only NEW records, from the point
                        of partition assignment onward
  "earliest"          → Starts from the earliest available offset
                        (after any cleanup/compaction)
  "none"              → Throws an Exception
```

## Fetch mechanics (recap from Part 2)

```java
while (true) {
    ConsumerRecords records = consumer.poll(timeout);
    for (ConsumerRecord record : records) {
        // business logic
    }
}
```

```
C1 talking to Broker1 and Broker2:
  FetchRequest for (P0, P2) → Broker1
  FetchRequest for (P1)     → Broker2
  (1 request per broker)

FetchRequest shape:
  Topic: order-events
    Partition: P0, Offset: X
    Partition: P2, Offset: Y

FetchResponse shape:
  Topic: order-events
    Partition: P0, Records
    Partition: P2, Records

Responses from B1 and B2 get merged into ONE ConsumerRecords object:
  Topic: order-events, Partition P0:
    ConsumerRecord(offset=10, value=A)
    ConsumerRecord(offset=11, value=B)
  Topic: order-events, Partition P1:
    ConsumerRecord(offset=5, value=C)
    ConsumerRecord(offset=20, value=D)
    ConsumerRecord(offset=21, value=E)
```

## Step 8 & 9 — Fetching from the Leader Broker

```
Checks the metadata, invokes the Leader Broker of Topic "order-events"
Partition-0, offset: xyz → to fetch from the given offset (from the
previous step).

Doubt: What if there's no event? How long will the consumer wait?

Kafka Consumer is single-threaded — it processes records one at a time.
During poll(timeout), we can set the timeout: the consumer waits AT
MOST this long for brokers to respond, else it proceeds with whatever
it received (or an empty list).

  spring.kafka.listener.poll-timeout=5000   # 5 seconds
```

### How much data is sent in one poll()?

```
We know a consumer can ask a broker to return data from multiple
partitions/topics in ONE request. But how much data will the broker
actually send back?

# Max data the Broker returns PER partition (1MB)
spring.kafka.consumer.properties.max.partition.fetch.bytes=1048576

# Maximum TOTAL data the Broker can return in one request (52MB)
spring.kafka.consumer.properties.fetch.max.bytes=52428800

# Minimum data (bytes) the Broker must have accumulated before responding
spring.kafka.consumer.properties.fetch.min.bytes=1

# How long the Broker can hold the request open to meet fetch.min.bytes (0.5s)
spring.kafka.consumer.properties.fetch.max.wait.ms=500

# Max number of records returned in a single poll() call
spring.kafka.consumer.properties.max.poll.records=500
```

```
Example: FetchRequest asks for Topic-A/P0 (offset X), Topic-A/P1
(offset Y), Topic-B/P1 (offset Z) — broker fetches up to
max.partition.fetch.bytes (1MB) PER partition, and the combined total
across all partitions must stay <= fetch.max.bytes (52MB).

Broker decides WHEN to respond:
  Condition 1: accumulated data >= fetch.min.bytes → respond immediately
  Condition 2: fetch.max.wait.ms (500ms) timeout reached → respond
               anyway with whatever's accumulated so far

Note: the broker's response is limited by BYTES, not by record count.
So it's entirely possible fetch.max.bytes=52MB gets filled by, say,
2000 records in one response.
```

### So what does `max.poll.records` actually do then?

```
Consumer receives the data from the broker into an internal buffer.
If that buffer contains MORE than 500 records, the consumer applies
max.poll.records=500 and returns AT MOST 500 records from poll().

The remaining records stay in the buffer. On the NEXT poll() call, the
consumer first checks this buffer — if records are already there, it
serves them WITHOUT making a new network call to the broker.
```

### Gaps between poll() calls

```
Doubt: Is it possible to have a gap between poll() calls?

# Sleep time between two poll() calls (default 0ms = as soon as possible)
spring.kafka.listener.idle-between-polls=0
```

### Detecting a stuck / dead consumer

```
Doubt: How do we know if a consumer is stuck while processing events,
or has some other issue?

# Consumer sends a heartbeat pulse to the Group Coordinator every 3s
spring.kafka.consumer.properties.heartbeat.interval.ms=3000

# Max time Group Coordinator waits for a heartbeat before declaring the
# consumer dead and starting a rebalance (generally higher than
# heartbeat.interval.ms)
spring.kafka.consumer.properties.session.timeout.ms=45000

# Group Coordinator expects 1 poll() call at least every 5 min. If the
# consumer takes, say, 6 minutes to process a batch (i.e. doesn't call
# poll() again in time), the coordinator assumes it's stuck and
# initiates a rebalance
spring.kafka.consumer.properties.max.poll.interval.ms=300000
```

## Step 10 — Consumer processes events

```
Doubt: What if an exception comes during Deserialization?
Doubt: What if an exception comes during processing the record?

→ Already covered in the Consumer Exception Handling notes.
```

## Step 11 — Offset commit

### Auto-commit

```properties
# Kafka commits offsets every 5 seconds via a background thread
spring.kafka.consumer.enable-auto-commit=true
spring.kafka.consumer.properties.auto.commit.interval.ms=5000
```

### Manual-commit — `ack-mode` options

```properties
spring.kafka.consumer.enable-auto-commit=false
spring.kafka.listener.ack-mode=<batch|record|time|count|manual|manual_immediate>
```

```
batch (default):
  poll() gets 500 records → process ALL 500 → commit ONCE.
  Con: if the LAST record in the batch fails, the ENTIRE batch is retried.

record:
  Process record1 → commit. Process record2 → commit. (commit per record)
  Con: much more commit overhead (one commit request per record...
       or is it? see manual vs manual_immediate below).

time:
  spring.kafka.listener.ack-mode=time
  spring.kafka.listener.ack-time=5000
  → Commits every 5 seconds.

count:
  spring.kafka.listener.ack-mode=count
  spring.kafka.listener.ack-count=10
  → Commits after every 10 records.

manual:
  @KafkaListener
  public void process(ConsumerRecord rec, Acknowledgment ack) {
      process(rec);       // business logic
      ack.acknowledge();  // commit
  }
  Even though we call ack.acknowledge() after each record, internally
  Spring just WAITS for the current poll() batch to finish, then sends
  ONE commit request to Kafka (batched under the hood).

manual_immediate:
  @KafkaListener
  public void process(ConsumerRecord rec, Acknowledgment ack) {
      process(rec);       // business logic
      ack.acknowledge();  // commit
  }
  For EACH call to ack.acknowledge(), it immediately sends a separate
  commit request to Kafka (no batching/waiting).
```
