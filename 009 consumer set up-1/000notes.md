# Consumer Setup — Part 1 [00:00]

## Complete Consumer Read Flow (recap, step by step) [00:00]

```
Step1: Consumer starts, wants to join Group
       group.id = "notification-service"

Step2: hash("notification-service-group-id") % 50 = 23
       → Partition 23 of internal topic "_consumer_offsets"

Step3: Consumer requests metadata (first time or refresh)
       Finds the partition number of topic "_consumer_offsets"

       Broker-1 (any broker) → Active Controller: "Give me metadata:
         Broker vs partition (leader)"
       Active Controller → Metadata response:
         topic: _consumer_offsets
           partition 0:  leader = Broker 1
           partition 2:  leader = Broker 2
           partition 23: leader = Broker 3

Step4: Invokes Broker3 (Group Coordinator) and requests to join the
       "notification-service" group

Step5: Broker3 (Group Coordinator)
         Group: notification-service
         Topic: order-events
           Consumer1: handle Partition-0
           Consumer2: handle Partition-1
           Consumer3: handle Partition-2
       "Join group" — once all followers get the latest update, Group
       Coordinator responds: "Partition-2 assigned for topic order-events"

       Broker1 → Partition 23 (follower)
       Broker2 → Partition 23 (follower)
       Followers do continuous polling and ACK once they've updated
       their partition logs.

       Broker3 waits for ALL followers to update the events in topic
       "_consumer_offsets" partition 23. There is NO option to configure
       ack=0/1/all for this — internally consumer commit behaves like
       ack=all.

Step6: Consumer fetches last committed offset for Topic "order-events"
       Partition-2

Step7: Broker3 (Group Coordinator) looks at "_consumer_offsets"
       Creates a key: group.id_topic_Partition
       key: "notificationGroupId_order-events_Partition-2"
       value: offset 100

       Note: Consumer offset details are NOT cluster metadata. They're
       stored like normal topic data — NOT in Controller nodes.

       Gets metadata (till where offset is processed) → returns 100

Step8: Checks the metadata and invokes the Leader Broker of Topic
       "order-events" Partition-2

Step9: Broker2 (leader of Topic "order-events" Partition-2)
       Fetch from offset 101, max bytes: 200 bytes
       Returns offset 101-501 events

Step10: Consumer processes events

Step11: Consumer commits offset (manual, batch-wise)

Step12: Broker3 (Group Coordinator) writes to _consumer_offsets,
        Partition 23: (group, topic, partition) → offset processed
        till = 501. Commit → ACK.

Step13: Continuous polling — move back to Step 8
```

---

## Step 1 — Dependency [08:25]

```xml
<!-- pom.xml -->
<dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka</artifactId>
</dependency>
```

## Step 2 — application.properties [09:40]

```properties
server.port=8082
spring.application.name=kafka-consumer-service
spring.kafka.bootstrap-servers=localhost:9092,localhost:9192

# Which group this consumer needs to join
spring.kafka.consumer.group-id=order-consumer-group

spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer

# Since we selected JsonDeserializer, we must tell it the default type
# to convert the JSON into
spring.kafka.consumer.properties.spring.json.value.default.type=com.eda.consumer.model.Order
```

```
group-id → should match whatever grouping we intend (independent of
producer, but the topic must match what the Producer publishes to).

value-deserializer=JsonDeserializer → needs
spring.json.value.default.type so it knows which POJO to deserialize
JSON into.
```

```java
// Order.java (POJO)
public class Order {
    private String orderId;
    private String customerId;
    private String productId;
    private Integer quantity;
    private Double totalAmount;
    private String status;
    // getters and setters
}
```

---

## What Spring Kafka does for us under the hood (conceptually, without Spring Boot) [24:47]

```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092,localhost:9192");
props.put("group.id", "order-consumer-group");
props.put("key.deserializer", StringDeserializer.class);
props.put("value.deserializer", JsonDeserializer.class);
props.put("auto.offset.reset", "latest");
props.put("enable.auto.commit", "true");

KafkaConsumer consumer = new KafkaConsumer<>(props);
consumer.subscribe(Arrays.asList("order-events"));
// Connect with 1 of the brokers and fetch cluster metadata, store it

try {
    while (true) {
        ConsumerRecords records = consumer.poll();

        // commitIntervalPassed default is 5 sec — after every 5s,
        // an offset commit request is made
        if (autoCommitEnabled && commitIntervalPassed) {
            commitOffsets();
        }

        for (ConsumerRecord record : records) {
            // business logic
        }

        // If enable.auto.commit=false, we must call manually:
        //   consumer.commitSync();  // blocks until broker confirms
        //   consumer.commitAsync(); // fire-and-forget, no guarantee
    }
} finally {
    consumer.close(); // leaves consumer group
    // No heartbeat is sent by the consumer's background thread while
    // closing → broker considers it dead → triggers rebalance
}
```

### What `poll()` actually does internally [28:00]

```
If it's the FIRST call:
  1. Sends JoinGroupRequest to Group Coordinator
  2. Receives partition assignment
  3. Requests last committed offset for the assigned partition(s)
     from the group coordinator
  4. Group coordinator reads from _consumer_offsets and returns the
     offset that needs to be read
  5. Sends FetchRequest to partition leaders with the offset it wants
     to read

If NOT the first call:
  → For subsequent calls, sends FetchRequest for new data
    (continuously increasing offsets)
```

```
Auto-commit behavior:
  If auto-commit is enabled, then after a specific time interval
  (commitIntervalPassed, default 5s), the offset is committed —
  regardless of whether the fetched/polled records were successfully
  processed or not!

  If auto-commit is false, we must manually call commitSync() or
  commitAsync() after processing.
```

---

## The Spring Boot way — same properties, but framework does the wiring [34:00]

```properties
# application.properties
spring.kafka.bootstrap-servers=localhost:9092,localhost:9192
spring.kafka.consumer.group-id=order-consumer-group
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer
spring.kafka.consumer.properties.spring.json.value.default.type=com.eda.consumer.model.Order
spring.kafka.consumer.auto-offset-reset=latest    # default
spring.kafka.consumer.enable-auto-commit=true     # default
```

```
Building blocks (who provides what):

  ConsumerFactory
    → We provide config (via application.properties)
    → DefaultKafkaConsumerFactory uses it to create a KafkaConsumer
      object under the hood

  ConcurrentKafkaListenerContainerFactory → Framework provided
    → holds a reference to ConsumerFactory

  @KafkaListener → We provide (on our method)
    → For each record (event), this annotated method gets invoked

  KafkaListenerContainer → Framework provided
    → gets the actual KafkaConsumer object from the ConsumerFactory
    → runs the poll loop internally
```

## Step 3 — Which topic to listen to + business logic [17:20]

```java
@Component
public class OrderEventListener {

    @KafkaListener(topics = "order-events")
    public void consume(Order order) {
        // business logic
        System.out.println("Received order event: " + order.getOrderId());
    }
}
```

```
@KafkaListener(topics = "order-events") → topic the consumer is
interested in. For each record (event) on that topic, this method
gets executed automatically.
```

## Consumer setup demo — missing from notes [18:09]

The video verifies the configuration against the running Kafka cluster:

1. Start the controllers and brokers, then start the consumer service containing the `@KafkaListener`.
2. Describe the `order-events` topic to verify its partitions and leaders.
3. Publish an order event (the demo uses order ID `32700`) and inspect the relevant partition log.
4. The listener receives the newly published event and runs its business logic.
5. For a brand-new consumer group with no committed offset, `auto.offset.reset=latest` starts from the latest position. Existing older records are not replayed; new records published after the consumer starts are consumed.

---

## Going 1 level deeper — what happens behind the scenes on startup [38:00]

```
1. Spring Boot Application starts
2. KafkaAutoConfiguration is triggered (because spring-kafka is on the classpath)
3. Creates Bean: ConsumerFactory
     → reads spring.kafka.consumer.* properties, stores them as a Map
4. Creates Bean: ConcurrentKafkaListenerContainerFactory
     → holds a reference to ConsumerFactory
     → NO actual KafkaConsumer object is created yet at this point
5. @EnableKafka is automatically added by Spring Boot (based on classpath)
     → only then Kafka listener processing gets enabled
6. Creates Bean: KafkaListenerAnnotationBeanPostProcessor
     → scans the app for @KafkaListener annotations
7. For each @KafkaListener found, it asks
   ConcurrentKafkaListenerContainerFactory to create a
   KafkaListenerContainer bean
8. KafkaListenerContainer gets an actual KafkaConsumer object from the
   ConsumerFactory
9. Poll loop starts
```

## Closing notes — missing from notes [46:31]

The consumer setup is now complete: configuration creates the `ConsumerFactory`, Spring builds a listener container for `@KafkaListener`, the container obtains the real `KafkaConsumer`, and its poll loop performs group joining, offset lookup, fetching, processing, and offset commits. The following consumer lessons build further on these properties and behaviors.
