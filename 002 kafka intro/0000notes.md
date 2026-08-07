![alt text](image-6.png)


**"Distributed event streaming platform that allows us to publish, store, and subscribe to events"**

### Word by word

**Distributed** — Kafka doesn't run on one server. It runs as a cluster of multiple brokers. If one crashes, the others keep going. This is what makes it reliable at scale.

**Event** — a record of something that happened in your system. `OrderPlaced`, `UserSignedUp`, `PaymentFailed`. Immutable — once written, never changed.

**Streaming** — events flow continuously in real time, not in scheduled batches. The moment OrderService publishes an event, InventoryService can start consuming it — not 5 minutes later.

**Platform** — it's not just a queue library. It's a full system with brokers, topics, partitions, consumer groups, replication, and APIs all built in and managed together.


![alt text](image-7.png)



---

### One sentence summary


> *"Kafka is a distributed system that runs across multiple servers, where services can publish events, Kafka stores them in an ordered log on disk, and any number of other services can subscribe to read and react to those events — all in real time."*


### Core Kafka terms

**Producer** — any service that writes events to Kafka. It picks a topic, optionally a partition key, and calls `send()`.

**Topic** — a named category of events, like a folder. All `order-events` go into one topic, all `user-events` into another. A topic lives across one or more brokers.It do not hold the events. Many producer can write to same topic.

**Partition** — a topic is split into partitions, each an independent ordered log. This is what enables parallelism. Events with the same key always go to the same partition, guaranteeing order for that key. A topic can have many partitions. Events are stored on disk in partitions so this is actually where events are stored.

![alt text](image-9.png)

![alt text](image-10.png)

![alt text](image-11.png)

see partitions are having log file.

**Offset** — the position number of an event within a partition. Offsets are immutable and permanent. Offset 5 in Partition 0 is always the same event, forever.It is incremental. Parition assigns an offset to event.

Each partition has its own offset ,Within a single partition order is guaranteed. So from a parition consumer consumes in incremental manner.

![alt text](image-12.png)

![alt text](image-13.png)
Order cant be mainatined globally,event in partition1 happened before event in partition2 cant be guarnateed.

**Broker** — a single Kafka server. A cluster has multiple brokers for redundancy. Each broker holds some partitions as leader, and some as replicas.

**Replica / Leader** — each partition has one leader broker (handles reads/writes) and N replica brokers (copies for fault tolerance). If the leader dies, a replica is elected.


**ZooKeeper / KRaft** — manages cluster metadata: which broker is the leader, which consumers are alive, what topics exist. Modern Kafka (2.8+) is replacing ZooKeeper with its own KRaft protocol.

**Consumer** — any service that reads events from a topic by polling Kafka and tracking its offset.

**Consumer group** — a named group of consumers that share reading a topic. Kafka ensures each partition goes to exactly one consumer in the group — this is how you scale consumers horizontally without duplicating work.

**Consumer offset** — each consumer group tracks *its own* offset per partition. Two groups reading the same topic each have independent offsets — they never interfere with each other.

---



![alt text](image-14.png)


![alt text](image-15.png)

In log file of partition-2  E5 is apended!!

Till now we have seen partition has log file and now see instead of having 1 big log file it has segment haing offset ranges.



```
kafka-logs/                          ← Kafka data directory
│
└── order-events/                    ← TOPIC  (your "folder")
    │
    ├── order-events-0/              ← PARTITION 0  (your "subfolder")
    │     ├── 00000000000000000000.log   ← segment: offsets 0 → 999,999
    │     ├── 00000000000001000000.log   ← segment: offsets 1,000,000 → 1,999,999
    │     ├── 00000000000002000000.log   ← segment: offsets 2,000,000 → now
    │     └── 00000000000000000000.index ← index file (fast offset lookup)
    │
    ├── order-events-1/              ← PARTITION 1  (your "subfolder")
    │     ├── 00000000000000000000.log
    │     └── 00000000000001000000.log
    │
    └── order-events-2/              ← PARTITION 2  (your "subfolder")
          └── 00000000000000000000.log
```

---

### Why Kafka splits into multiple segment files

A partition does not use one giant file. It splits into segments because:

```
One giant file problem:
  - Can't delete old events (retention) without rewriting everything
  - Slow to search through millions of records

Segment files solution:
  - Old segment expired? Just delete that one file. Fast.
  - Kafka reads the index file to jump directly to any offset
  - Active segment = the last file, append-only writes at the end
```

Think of it like a logbook — instead of one massive book, you use one book per month. When January's book is old, you throw it away without touching February or March.

---

### The naming tells you the offset range

```
00000000000000000000.log  ← starts at offset 0
00000000000001000000.log  ← starts at offset 1,000,000
00000000000002000000.log  ← starts at offset 2,000,000

The file name IS the first offset in that segment.
Kafka uses this to instantly find which file contains offset 1,500,000
→ must be in 00000000000001000000.log
```





![alt text](image-16.png)


![alt text](image-17.png)

 there is an index file too ,it tells which offset you find which position or byte.We tell after how much byte store index.

![alt text](image-19.png)

Every segment has a matching `.index` file alongside the `.log` file.

The index file is what makes Kafka **blazing fast** at finding any offset.

### The complete file pair per segment

Every segment on disk is always **two files together** — never one without the other:

```
order-events-0/
  ├── 00000000000000000000.log      ← actual event data
  ├── 00000000000000000000.index    ← offset → byte position map
  ├── 00000000000000000000.timeindex ← timestamp → offset map (bonus!)
  │
  ├── 00000000000001000000.log
  ├── 00000000000001000000.index
  ├── 00000000000001000000.timeindex
  │
  ├── 00000000000002000000.log      ← active segment (writes happen here)
  ├── 00000000000002000000.index    ← grows as new events arrive
  └── 00000000000002000000.timeindex
```

There is also a `.timeindex` file — same idea but maps **timestamp → offset** instead of offset → byte. This is how Kafka lets you seek by time: `consumer.seekToTimestamp("2026-05-24 10:00:00")`.

---

### Why the index is sparse (not every offset)

```
offset=0    → byte 0
offset=100  → byte 4,820      ← checkpoint every ~100 offsets
offset=200  → byte 9,640
offset=300  → byte 14,460

NOT:
offset=0   → byte 0
offset=1   → byte 48          ← this would make the index huge
offset=2   → byte 96
...
```

Storing every single offset in the index would make it massive. Instead Kafka stores a checkpoint every few kilobytes. So the worst case is a tiny sequential scan of maybe 50-100 records after jumping to the nearest checkpoint — still essentially instant.

---



```
kafka-logs/
└── order-events/              ← Topic     (folder)
    └── order-events-0/        ← Partition (subfolder)
        ├── 0000...0000.log    ← Segment   (chunk of event data)
        ├── 0000...0000.index  ← Index     (offset → byte map for fast lookup)
        └── 0000...0000.timeindex  ← Time index (timestamp → offset map)
```



![alt text](image-20.png)

offset 4 is put in segment 1 as `4300>4096`


## How producer decides a event goes to which partition??



![alt text](image.png)


![alt text](image-1.png)

![alt text](image-2.png)




![alt text](image-22.png)

So topic and partitions are distributed across multiple brokers.

One broker do not hold all topic .

If a topic is present it will not have all partitions of that topic.



