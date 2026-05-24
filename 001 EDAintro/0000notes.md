## Event-Driven Architecture (EDA)

Event-Driven Architecture is a design pattern where components communicate by **producing and consuming events** rather than calling each other directly. Components are loosely coupled — the producer doesn't know or care who's listening.

### Core Concepts

**Event** — A record of something that happened (e.g., `OrderPlaced`, `UserRegistered`)

**Producer** — Publishes events when something occurs

**Consumer** — Listens and reacts to events

**Event Bus / Broker** — The channel that routes events between producers and consumers


## What is an Event?

An **event** is simply a **record that something happened** in your system — a fact about the past, captured as an object.

Think of it like a notification: *"Hey, this thing just occurred."*

---

### Real-World Analogy

> When you place an order on Amazon:
> - Amazon doesn't directly call the warehouse, email team, and billing team one by one.
> - Instead, it fires an **`OrderPlaced`** event.
> - Each department *listens* and reacts independently.

The event is just the **message** carrying the facts: *what happened, when, and with what data.*

---

### In Java — An Event is Just a Class

```java
// Minimal event — just a POJO or record
public record OrderPlacedEvent(
    String orderId,
    String customerId,
    double totalAmount,
    LocalDateTime occurredAt   // when it happened
) {}
```

That's it. No logic. No methods. Just **data describing what happened.**

---

### Anatomy of a Good Event

```java
public record UserRegisteredEvent(
    String eventId,        // unique ID for this event
    String userId,         // who it's about
    String email,          // relevant data
    LocalDateTime at       // when it happened
) {}
```

| Field | Purpose |
|---|---|
| `eventId` | Uniquely identify this event |
| `userId` | The subject of the event |
| `email` | Data needed by consumers |
| `at` | Timestamp — events describe the **past** |

---

### Key Characteristics of an Event

**1. Immutable** — Events describe something that already happened; you can't change the past
```java
// Use record (immutable by default) — preferred
public record PaymentFailedEvent(String paymentId, String reason) {}
```

**2. Named in past tense** — Convention makes intent clear
```
✅  OrderPlaced, UserRegistered, PaymentFailed
❌  PlaceOrder, RegisterUser, FailPayment  ← these are commands, not events
```

**3. Self-contained** — Carries all the data a listener needs
```java
// Bad — consumer has to go fetch more data
public record OrderPlacedEvent(String orderId) {}

// Good — carries enough context
public record OrderPlacedEvent(String orderId, String customerId, double amount) {}
```

**4. Describes the past, not intent** — An event says *"this happened"*, not *"please do this"*

---

### Event vs Command vs Query

| | Command | Event | Query |
|---|---|---|---|
| **Example** | `PlaceOrder` | `OrderPlaced` | `GetOrderById` |
| **Intent** | Do something | Something happened | Fetch data |
| **Direction** | One target | Many listeners | One responder |
| **Tense** | Imperative | Past tense | Question |

---

### Summary

> An **event** = an **immutable, named, past-tense fact** about something that occurred in your system, packaged as a data object and broadcast so interested parties can react.

It's the foundation of EDA — everything else (producers, consumers, buses) exists just to create and handle events.


---

### Simple In-Memory Example (Pure Java)

```java
// 1. Define an Event
public record OrderPlacedEvent(String orderId, double amount) {}

// 2. Define a Consumer (Listener) interface
@FunctionalInterface
public interface EventListener<T> {
    void onEvent(T event);
}

// 3. Simple Event Bus
public class EventBus {
    private final Map<Class<?>, List<EventListener<?>>> listeners = new HashMap<>();

    public <T> void subscribe(Class<T> eventType, EventListener<T> listener) {
        listeners.computeIfAbsent(eventType, k -> new ArrayList<>()).add(listener);
    }

    @SuppressWarnings("unchecked")
    public <T> void publish(T event) {
        List<EventListener<?>> eventListeners = listeners.getOrDefault(event.getClass(), List.of());
        for (EventListener<?> listener : eventListeners) {
            ((EventListener<T>) listener).onEvent(event);
        }
    }
}

// 4. Wire it together
public class Main {
    public static void main(String[] args) {
        EventBus bus = new EventBus();

        // Subscribe consumers
        bus.subscribe(OrderPlacedEvent.class, e ->
            System.out.println("Email sent for order: " + e.orderId()));

        bus.subscribe(OrderPlacedEvent.class, e ->
            System.out.println("Inventory updated for: " + e.orderId()));

        // Publish an event
        bus.publish(new OrderPlacedEvent("ORD-001", 199.99));
    }
}
```

**Output:**
```
Email sent for order: ORD-001
Inventory updated for: ORD-001
```

---

### With Spring (Most Common in Production)

Spring makes EDA very clean with `@EventListener` and `ApplicationEventPublisher`.

```java
// 1. Event class
public record PaymentProcessedEvent(String paymentId, String userId) {}

// 2. Producer Service
@Service
public class PaymentService {

    private final ApplicationEventPublisher publisher;

    public PaymentService(ApplicationEventPublisher publisher) {
        this.publisher = publisher;
    }

    public void processPayment(String userId) {
        // ... payment logic ...
        publisher.publishEvent(new PaymentProcessedEvent("PAY-123", userId));
    }
}

// 3. Consumer Services (completely decoupled from producer!)
@Service
public class NotificationService {
    @EventListener
    public void onPayment(PaymentProcessedEvent event) {
        System.out.println("Notify user: " + event.userId());
    }
}

@Service
public class AuditService {
    @EventListener
    public void onPayment(PaymentProcessedEvent event) {
        System.out.println("Audit log: " + event.paymentId());
    }
}
```

For **async** handling (non-blocking), just add `@Async`:
```java
@Async
@EventListener
public void onPayment(PaymentProcessedEvent event) { ... }
```

---

### With Kafka (Distributed / Microservices)

For events that need to cross service boundaries:



## Events Crossing Service Boundaries

This means: **two completely separate applications/microservices need to communicate.**

---

### What is a "Service Boundary"?

Imagine your system is split into independent services, each with its own:
- Codebase
- Database
- Server / JVM process

```
┌─────────────────┐        ┌──────────────────────┐
│   Order Service │        │  Inventory Service    │
│   (Java App 1)  │        │  (Java App 2)         │
│   Port: 8081    │        │  Port: 8082           │
│   Its own DB    │        │  Its own DB           │
└─────────────────┘        └──────────────────────┘
```

These two apps **cannot share memory** — they run in separate JVM processes. So Spring's `@EventListener` (which works in-memory) **won't work here.**

---

### The Problem Without EDA

The naive solution is a **direct HTTP call:**

```java
// Order Service directly calling Inventory Service
@Service
public class OrderService {
    public void placeOrder(Order order) {
        // Tight coupling! Order Service must KNOW about Inventory Service
        restTemplate.post("http://inventory-service/reserve", order);
        restTemplate.post("http://notification-service/notify", order);
        restTemplate.post("http://billing-service/charge", order);
        // If ANY of these fail or are slow, the whole thing breaks
    }
}
```

**Problems:**
- ❌ Order Service is tightly coupled to 3 other services
- ❌ If Inventory Service is down, Order placement fails
- ❌ Hard to add a new service later

---

### The Solution — A Message Broker (e.g. Kafka)

A **broker sits in the middle** and decouples the services completely:

```
┌─────────────────┐        ┌─────────┐       ┌────────────────────┐
│  Order Service  │──────▶ │  Kafka  │──────▶│  Inventory Service │
│  (Producer)     │publishes│ (Broker)│consume│  (Consumer)        │
└─────────────────┘  event └─────────┘       └────────────────────┘
                                │
                                │─────────────▶ Notification Service
                                │
                                └─────────────▶ Billing Service
```

- Order Service **publishes once** to Kafka
- Every other service **independently consumes** the event
- Services don't know about each other at all

---

### In Code

**Order Service (App 1) — Producer:**
```java
@Service
public class OrderService {
    private final KafkaTemplate<String, String> kafka;

    public void placeOrder(Order order) {
        // Just fire the event and forget
        // Doesn't know or care who's listening
        kafka.send("order-events", order.getId(), toJson(order));
    }
}
```

**Inventory Service (App 2) — Consumer:**
```java
// Completely separate Spring Boot application
@Service
public class InventoryService {

    @KafkaListener(topics = "order-events", groupId = "inventory-group")
    public void handleOrder(String payload) {
        System.out.println("Reserving stock for: " + payload);
    }
}
```

**Notification Service (App 3) — Also a Consumer:**
```java
// Another separate Spring Boot application
@Service
public class NotificationService {

    @KafkaListener(topics = "order-events", groupId = "notification-group")
    public void handleOrder(String payload) {
        System.out.println("Sending email for: " + payload);
    }
}
```

---

### Why Kafka Specifically?

| Feature | Why it helps across boundaries |
|---|---|
| **Persistent log** | Events are stored; consumers can replay them |
| **Multiple consumers** | Many services read the same event independently |
| **Async** | Producer doesn't wait for consumers to finish |
| **Resilient** | Consumer can be down and catch up later |

---

### Simple Summary

> **Within one app** → use Spring `@EventListener` (in-memory)
>
> **Across separate apps** → use a broker like **Kafka or RabbitMQ** as the middleman, so services never directly talk to each other.

```java
// Producer (Order Service)
@Service
public class OrderService {
    private final KafkaTemplate<String, String> kafka;

    public void placeOrder(Order order) {
        kafka.send("order-events", order.getId(), toJson(order));
    }
}

// Consumer (Inventory Service — completely separate app)
@Service
public class InventoryService {
    @KafkaListener(topics = "order-events", groupId = "inventory-group")
    public void handleOrder(String payload) {
        System.out.println("Reserving stock for: " + payload);
    }
}
```

---

### Key Benefits

| Benefit | Why it matters |
|---|---|
| **Loose coupling** | Services don't reference each other |
| **Scalability** | Consumers scale independently |
| **Resilience** | One consumer failing doesn't break others |
| **Extensibility** | Add new listeners without changing producers |
| **Auditability** | Events form a natural history log |

---

### When to Use It

✅ **Good fit:** Microservices, async workflows, notifications, audit logging, real-time updates

❌ **Avoid when:** You need immediate response/return values, simple CRUD apps, or tight transactional consistency is required across steps

The most common Java stack for EDA is **Spring Boot + Apache Kafka** for distributed systems, or **Spring Events** for within a single application.


![alt text](image.png)


![alt text](image-1.png)



![alt text](image-2.png)

Here's the full sequence diagram combined into one image, matching both your screenshots. It shows all three phases:

- **Red box** — Real-time processing: the user places an order, inventory is checked synchronously, the order is saved as PENDING, and `OrderCreated` is published. The user gets an immediate "Order Accepted" response.
- **Green box** — Async & parallel processing: the Event Router pushes the `OrderCreated` event to both PaymentService and InventoryService simultaneously. They process in parallel and each publish their success events back.
- **Blue box** — Async processing (completion): the Event Router pushes the `PaymentSuccess` event to NotificationService (sends email/SMS) and back to OrderService (updates status to COMPLETED).




In EDA, **Push** and **Pull** are two different ways consumers receive events from the broker.

## Push vs Pull in EDA

The core difference is simple: **who initiates the data transfer?**

**Push** — the broker sends events to the consumer automatically as soon as they arrive. The consumer just waits and reacts.

**Pull** — the consumer goes to the broker and asks "do you have anything for me?" on its own schedule.

Here are the two models side by side:### Push — broker drives delivery

![alt text](image-3.png)


The broker actively delivers events to the consumer the moment they arrive. The consumer just listens.

```java
// Kafka with Push-style via Spring @KafkaListener
// Consumer registers once — broker pushes events to it automatically
@KafkaListener(topics = "order-events", groupId = "payment-group")
public void handleOrder(String event) {
    // broker called this — you didn't ask for it
    processPayment(event);
}
```

Real-world analogy: like getting an email notification. You don't check your inbox — the server pushes the alert to your phone the moment it arrives.

---

### Pull — consumer drives delivery

The consumer periodically asks the broker "do you have anything for me?" and fetches a batch on its own schedule. Kafka actually uses pull internally — `KafkaConsumer.poll()` is the key method.

```java
// Raw Kafka Consumer — Pull model
KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
consumer.subscribe(List.of("order-events"));

while (true) {
    // Consumer ASKS the broker — "give me up to 100ms worth of records"
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

    for (ConsumerRecord<String, String> record : records) {
        processOrder(record.value());
    }
    // Consumer decides when to poll again — full control
}
```

Real-world analogy: like manually refreshing your inbox. You decide when to check, and you get everything that accumulated since your last check.

---

### When to use which

Use push when you need real-time reaction and your consumer can keep up — notifications, live dashboards, fraud detection. Use pull when your consumer needs to control its own pace — batch processing, slow downstream services, or when you want to replay old events from a specific offset (Kafka's killer feature).

In practice, Kafka is fundamentally pull-based, while AWS SNS and WebSockets are push-based. Spring's `@KafkaListener` hides the polling loop from you, but under the hood it's still pull.

# Pub/sub vs streaming

 These are two fundamental patterns in EDA that are often confused.

**Pub/Sub** — fire and forget. Events are delivered to subscribers and then gone. Think notifications.

**Streaming** — events are stored in an ordered log permanently (or for a set time). Consumers can replay, rewind, and process at their own pace.

![alt text](image-4.png)

### The key difference in one line

> Pub/Sub says *"here's the event, catch it or miss it."* Streaming says *"here's the event, stored at offset 42 — read it whenever you want."*

---

### In Java code

**Pub/Sub with RabbitMQ** — message is gone once delivered:
```java
// Publisher fires and forgets
rabbitTemplate.convertAndSend("order.exchange", "order.created", event);

// Subscriber — if it's offline when message arrives, it's lost
@RabbitListener(queues = "order.queue")
public void handleOrder(OrderEvent event) {
    // got it — but only if we were online
}
```

**Streaming with Kafka** — events live in a log, consumers track their own offset:
```java
// Consumer A — processing live, currently at offset 150
@KafkaListener(topics = "orders")
public void handleLive(ConsumerRecord<String, String> record) {
    System.out.println("Offset: " + record.offset()); // e.g. 150
}

// Consumer B — brand new service, replays ALL history from the beginning
consumer.seekToBeginning(consumer.assignment()); // rewind to offset 0
// now processes every order ever placed — pub/sub can never do this
```

---

### When to choose which

Use **pub/sub** when the event only matters right now — sending a notification, triggering a webhook, alerting a dashboard. If the subscriber missed it, that's fine.

Use **streaming** when the event has lasting value — audit logs, analytics pipelines, ML training data, financial records, or any new service that needs to catch up on history. Kafka's ability to let any consumer replay from offset 0 is its superpower over pub/sub.

In many real systems you actually use both — Kafka for the durable event log, and something like SNS/RabbitMQ for lightweight real-time fan-out to things like mobile push notifications.


![alt text](image-5.png)

![alt text](image-6.png)






A **broker** is the middleman that sits between producers and consumers — it receives events, stores them, and delivers them. Neither side talks to each other directly; everything goes through the broker.


![](image-7.png)


**Real world analogy:** Think of a broker like a **post office**. The sender (producer) drops a letter there. The post office (broker) stores it and delivers it to the right recipient (consumer). The sender doesn't need to know where the recipient lives, and the recipient doesn't need to be home when the letter arrives.### The broker's 4 jobs

**1. Receive** — accepts events from any producer. The producer just says "here's an event for the `order-events` topic" and moves on.

**2. Store** — holds the event in a topic (Kafka stores to disk; RabbitMQ holds in memory by default). This is what allows consumers to be offline and still get the event later.

**3. Route** — figures out which topic the event belongs to and which consumers have subscribed to it.

**4. Deliver** — sends the event to the right consumers, either by pushing it to them or waiting for them to pull it.

---

### Without a broker (the problem)

```java
// Without broker — OrderService must know about EVERYONE
public void placeOrder(Order order) {
    inventoryService.reserve(order);      // tight coupling
    notificationService.notify(order);    // what if it's down?
    analyticsService.track(order);        // adding new service = change this code
    billingService.charge(order);         // 4 direct dependencies!
}
```

### With a broker (the solution)

```java
// With broker — OrderService knows about NO ONE
public void placeOrder(Order order) {
    broker.publish("order-events", order); // that's it. done.
    // inventory, notification, analytics all get it independently
    // adding a new service = zero changes here
}
```

---

### Popular brokers and when to use them

| Broker | Best for |
|---|---|
| **Apache Kafka** | High-volume streaming, replay, audit logs |
| **RabbitMQ** | Simple task queues, routing rules |
| **AWS SQS/SNS** | Cloud-native apps on AWS |
| **Google Pub/Sub** | Cloud-native apps on GCP |

The broker is the heart of every EDA system — everything else (push/pull, pub/sub, streaming) is just about *how* the broker stores and delivers events.






