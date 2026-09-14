

Consumer processes events, and there are exactly **2 places** an exception can happen:

```
1. Exception during Deserialization (of key or value)
2. Exception during Processing the record (inside the listener)
```

### High level flow

![alt text](image.png)

```
poll()
  → deserialize() each record
  → For each record: invoke listener and process the record
  → Commit offset

1st place exception can come:  during deserialization of key or value this is called poison pill problem

one bad message will stuck your consumer forever

2nd place exception can come:  during processing the record (event),null pointer exception can occur
```

---

## 1. Exception during Deserialization

### The problem — Infinite Loop

Poison pill is bad message

![alt text](image-1.png)

```
Producer sends: Value = String
Consumer expects: Value = Json

Broker offsets for topic order-events, partition 0:
  Offset 0: value is json
  Offset 1: value is json
  Offset 2: value is String   ← the buggy message

Consumer flow:
  poll() → fetches event at offset 2 → Deserialization exception
         → prints error stack trace → next offset to read still = 2

INFINITE LOOP: even if we stop and restart the consumer, it restarts
from offset 2 and gets stuck again, forever.
```

Broker log dump confirming the buggy record at offset 2:

![alt text](image-2.png)

```
Offset 0 & 1: normal JSON payloads (producerId 1000)
Offset 2: producerId 1001, payload: "this is not a json"  ← the culprit
```

Consumer group status shows the stuck lag:

![alt text](image-p1-2.png)

```
order-consumer-group / order-events / Partition 0:
  CURRENT-OFFSET: 2   LOG-END-OFFSET: 3   LAG: 1
  → 1 offset (the buggy one) is never successfully processed/committed.
```
![alt text](image-3.png)

![alt text](image-5.png)

---

### What is a Dead Letter Topic (DLT)?

A **Dead Letter Topic (DLT)** (derived from Dead Letter Queue / DLQ in messaging systems) is a secondary, dedicated Kafka topic used to store messages that cannot be processed successfully by a consumer.

#### Why do we need DLT?
1. **Unblocks the Consumer (Prevents Head-of-Line Blocking)**:
   In Kafka, partition order is strict. If record at offset `N` fails deserialization or processing, the consumer cannot advance to offset `N+1`. Moving the bad record to a DLT allows the consumer to commit offset `N` and keep processing healthy messages.
2. **Zero Data Loss**:
   Instead of dropping or ignoring corrupt messages, storing them in DLT guarantees an audit trail.
3. **Triage, Debugging & Replay**:
   Engineers can inspect bad payloads, fix the underlying consumer logic or producer bug, and replay the messages back into the main topic.

```
Normal Flow:
  [Main Topic] ───────> Consumer ───────> Process Record ───> Commit Offset

Failure Flow with DLT:
  [Main Topic] ───────> Consumer ───────> Deserialization / Processing Fails
                                                │
                                                ▼ (after retries / fatal error)
                                     Publish to [DLT Topic]
                                                │
                                                ▼
                                           Commit Offset
                              (Consumer advances to next offset!)
```

```
Headers enriched when publishing to DLT:
  - kafka_dlt-original-topic
  - kafka_dlt-original-partition
  - kafka_dlt-original-offset
  - kafka_dlt-exception-fqcn (exception class)
  - kafka_dlt-exception-message
  - kafka_dlt-exception-stacktrace
```

---

### Solution — Error Handling wrapper

![alt text](image-4.png)

```
Flow with the wrapper:

Deserialization logic → Deserialization Exception?
  Yes → ErrorHandler is invoked
  No  → For each record, invoke listener and process the record → Commit offset

Default config of ErrorHandler:
  0 retries — it's treated as a FATAL exception (no matter how many
  times you retry, it will fail the same way)
  No failure event stored in DLT(Dead Letter topic) — just error logging and move to next
  → offset is still committed after logging
```

Add the deserializer wrapper (Consumer `application.properties`):

```properties
# Use wrapper
spring.kafka.consumer.key-deserializer=org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.ErrorHandlingDeserializer

# wrapper wraps the actual delegate deserializer class
spring.kafka.consumer.properties.spring.deserializer.key.delegate.class=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.properties.spring.deserializer.value.delegate.class=org.springframework.kafka.support.serializer.JsonDeserializer
```

```
At this point we haven't added a custom Error Handler bean — we're
relying on the framework's DefaultErrorHandler. That's it: the wrapper
plus the default handler is all managed by the framework.

DefaultErrorHandler gets invoked; for deserialization errors it uses
FixedBackOff with maxAttempts = 0 → do not retry if it fails once.
```

The actual exception chain seen in logs (RecordDeserializationException → SerializationException → JsonParseException):

![alt text](image-p2-3.png)

The DefaultErrorHandler exhausting its (zero) backoff and failing the listener invocation:

![alt text](image-6.png)

```
No lag afterwards — meaning the consumer did commit the offset for the
record that failed deserialization. So the consumer isn't stuck anymore,
BUT we permanently lost that event/record.

In production we should NOT rely on DefaultErrorHandler for this.
Instead, we should use our own Error Handler that stores the failed
event in a DLT (Dead Letter Topic) or in DB or alert, so that after a fix, it can be
replayed (or some other action taken on it).
```

### Retry strategies — configuring the Error Handler Bean

```java
@Bean
public DefaultErrorHandler errorHandler() {
    DefaultErrorHandler handler = new DefaultErrorHandler(
        new FixedBackOff(1000L, 2));
    return handler;
}
```

```
FixedBackOff(1000L, 2): wait 1000ms (1 sec) before retrying, max 2 retries.

Attempt1: fail
Wait 1 sec
Attempt2 (Retry-1): fail
Wait 1 sec
Attempt3 (Retry-2): fail
```

```java
@Bean
public DefaultErrorHandler errorHandler() {
    ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(5);
    backOff.setInitialInterval(1000L);
    backOff.setMultiplier(2.0);
    backOff.setMaxInterval(10000L);

    DefaultErrorHandler handler = new DefaultErrorHandler(backOff);
    return handler;
}
```

```
Two retry strategies:
  1. FixedBackOff
  2. ExponentialBackOffWithMaxRetries

ExponentialBackOffWithMaxRetries(5):
  First retry after 1s delay, each subsequent retry doubles the wait (*2)
    Retry 1: 1s wait
    Retry 2: 2s wait
    Retry 3: 4s wait
    Retry 4: 8s wait
  Max delay capped at 10s (if multiplied delay > 10s, use 10s instead)
  Max retries = 5 only
```

### Recoverer — what happens after ALL retries are exhausted?

![alt text](image-7.png)

```
ConsumerRecordRecoverer            (Functional Interface)
  void accept(T t, U u);

  extends → ConsumerAwareRecordRecoverer   (Functional Interface)
              void accept(ConsumerRecord<?, ?> record, Exception exception)

  implements → DeadLetterPublishingRecoverer   (Class, framework-provided)
                 void accept(ConsumerRecord<?, ?> record, Exception exception) {
                     // logic to insert record in DLT
                 }
```

![alt text](image-8.png)

order-event-dlt is DLT topic so naming is very important as `DeadLetterPublishingRecoverer` looks the topic that ends with `<failed-evenr-name>-dlt` so here event name was `order-event`  which got failed appended by `-dlt`

In recoverer bean we putting template as for producing an event we need `KafkaTemplate`

then we put errorHandler in that we put recoverer.

### How DLT actually works internally

![alt text](image-9.png)



```
Deserialization issue happens
  → ErrorHandler wrapper intercepts:
      - Creates a DeserializationException object (full exception details)
      - Creates a ConsumerRecord with:
          Value = null (if value deserialization failed) else actual data
          Key   = null (if key deserialization failed) else actual data
          Header contains the exception + RAW bytes:
            springDeserializerExceptionKey   = key raw bytes
            springDeserializerExceptionValue = value raw bytes
          (whichever either key or value or both failed has its raw bytes stashed in a header)
  → After wrapper it comes to exception handler where retries happens,after all retries is exhausted then recoverer is there,which calls accept method and put record and exception generated by wrapper

  → In DeadLetterPublishingRecoverer.accept(record, exception) is called,both record and exception created by wrapper only
  → Insert into topicName-dlt topic
```

```text
Example 1 — value deserialization failed:
  ConsumerRecord: Value = null, Key = "O-111"
  Header: springDeserializerExceptionValue = bytes[]

Example 2 — key deserialization failed:
  ConsumerRecord: Value = Order Object, Key = null
  Header: springDeserializerExceptionKey = bytes[]
```
See accept method code,it is very big code we have simplied version
```java
  public void accept(ConsumerRecord<?, ?> record, Exception exception) {
          // 1. Find the raw bytes from the exception
          byte[] rawKeyBytes = ((DeserializationException) exception)
              .getHeader("springDeserializerExceptionKey");
          byte[] rawValueBytes = ((DeserializationException) exception)
              .getHeader("springDeserializerExceptionValue");

          // 2. Create a NEW record for the DLT topic
          ProducerRecord<Object, Object> dltRecord = new ProducerRecord<>(
              record.getTopic() + "-dlt",                                  // destination topic
              record.key()   != null ? record.key()   : rawKeyBytes,       // fallback to raw bytes if key failed
              record.value() != null ? record.value() : rawValueBytes);    // fallback to raw bytes if value failed

          // 3. send it
          kafkaTemplate.send(dltRecord);
      }
```

We do not need to write everything 

## What we need to do??

Only wrapper in application.properties


### Wiring up the DLT feature

```java
@Bean
public NewTopic orderDltEventsTopic() {
    return TopicBuilder.name("order-events-dlt").build();
}

@Bean
public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(KafkaTemplate<Object, Object> template) {
    return new DeadLetterPublishingRecoverer(template);
}

@Bean
public DefaultErrorHandler errorHandler(DeadLetterPublishingRecoverer recoverer) {
    DefaultErrorHandler handler = new DefaultErrorHandler(recoverer,
        new FixedBackOff(1000L, 2));
    return handler;
}
```

```
- Created a DLT topic ("order-events-dlt") to store failed events.
- Created a DeadLetterPublishingRecoverer bean — this framework class
  already has all the code needed to push failed records to the DLT.
  All we need to give it is a KafkaTemplate (its serializers etc. come
  from application.properties).
- Now the Error Handler has BOTH a Retry feature AND a DLT fallback:
    after all retries are exhausted, the handler invokes the
    Recoverer's accept(record, exception) method.

    public DefaultErrorHandler(ConsumerRecordRecoverer recoverer, BackOff backOff) { ... }

- DeadLetterPublishingRecoverer is the framework-provided implementation.
  We can also write our OWN recoverer instead — e.g. insert the failed
  record into a DB instead of a DLT, or send an alert.
```

The value of key and value can be either the data or bytes[] so we need jsonDesrializer for first and ByteDeserializer for second ,so what to use??

### The tricky part — what serializer to use for the DLT topic?


Usecase 1 — value deserialization failed:

```
  ConsumerRecord: Value = null, Key = "O-111"
  Header: springDeserializerExceptionValue = value bytes[]

  ProducerRecord<Object, Object> dltRecord = new ProducerRecord<>(
      "order-events-dlt", "O-111", valueBytes);

  # Serializers needed:
  spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
  spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.ByteArraySerializer
```
Usecase 2 — key deserialization failed:


```
  ConsumerRecord: Value = Order Object, Key = null
  Header: springDeserializerExceptionKey = key bytes[]

  ProducerRecord<Object, Object> dltRecord = new ProducerRecord<>(
      "order-events-dlt", keyBytes, Order);

  # Serializers needed:
  spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.ByteArraySerializer
  spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer
```


Challenge: usecase 1 and usecase 2 need DIFFERENT serializer configs for
key vs value — but we can only configure ONE pair of serializers
globally in application.properties.

```
Pragmatic take: most of the time the KEY is a plain String, so
deserialization issues on the key are rare. Deserialization issues
mostly happen on the VALUE (JSON parsing). So this combination usually
works fine:

  spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer

  spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.ByteArraySerializer
```

But if the KEY can also be an Object, the issue could be on:
  - only key
  - only value
  - both

Then choosing between ByteArraySerializer and JsonSerializer up front
becomes genuinely hard.

Best answer: override the recoverer's record-creation method and ALWAYS
convert both key and value to bytes before publishing to the DLT — then
use ByteArraySerializer for BOTH key and value, unconditionally:

```properties
  spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.ByteArraySerializer

  spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.ByteArraySerializer
```

```java
@Bean
public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(KafkaTemplate<Object, Object> template) {

    DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template) {

        // Just override this method and convert key and value to bytes
        @Override
        protected ProducerRecord<Object, Object> createProducerRecord(
                ConsumerRecord<?, ?> record, TopicPartition tp, Headers headers,
                byte[] key, byte[] value) {

            Object finalKey   = (key   != null) ? key   : objectMapper.writeValueAsBytes(record.key());
            Object finalValue = (value != null) ? value : objectMapper.writeValueAsBytes(record.value());

            return new ProducerRecord<>(tp.topic(), tp.partition(), finalKey, finalValue, headers);
        }
    };
    return recoverer;
}

@Bean
public DefaultErrorHandler errorHandler(DeadLetterPublishingRecoverer recoverer) {
    DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2));
    return handler;
}
```

---

## Custom business logic

if we want to save to DB

Custom retryer added straight into the Error Handler (instead of a DLT recoverer):

```java
@Bean
public DefaultErrorHandler errorHandler(DeadLetterPublishingRecoverer recoverer) {
    DefaultErrorHandler handler = new DefaultErrorHandler(
        (record, exception) -> {
            System.out.println("Failed record: " + record);
            //add business logic here if you want to add something
        },
        new FixedBackOff(1000L, 2));
    return handler;
}
```

## 2. Exception during Processing the record

![alt text](image-10.png)

```
Flow: Producer → Broker → Consumer deserializes fine → poll() →
Process record → throws exception (e.g. NullPointerException)
```

Log output when this custom recoverer fires (failed record printed with full detail, including headers):

![alt text](image-p6-6.png)

```
Default Error Handler behavior for a processing exception:
  - Logs it
  - Commits the offset (so the consumer doesn't get stuck)

Key difference from deserialization errors:
  - For deserialization issues, we MUST add the ErrorHandlingDeserializer
    wrapper ourselves for the DefaultErrorHandler to even kick in.
  - For processing exceptions, the DefaultErrorHandler is active by
    default — no wrapper needed.

If we want Retry + a final action once retries are exhausted (like DLT)
for processing exceptions too — it's exactly the same setup we already
did for deserialization (FixedBackOff / ExponentialBackOffWithMaxRetries
+ a Recoverer bean, whether that's DeadLetterPublishingRecoverer or a
custom one).
```
