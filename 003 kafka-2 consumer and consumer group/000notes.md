![alt text](image-1.png)

![alt text](image-9.png)



`Within a single consumer group, partition assignments to consumers are mutually exclusive.`

or 

`intersection of  partitions assigned to  consumers of a consumer group is null`

### The 3 golden rules 

```
Rule 1 — consumers < partitions:
  2 consumers, 3 partitions → one consumer gets 2 partitions
  Works but unbalanced

Rule 2 — consumers = partitions:  ← IDEAL
  3 consumers, 3 partitions → each gets exactly 1
  Maximum parallelism, perfectly balanced

Rule 3 — consumers > partitions:  ← WASTEFUL
  4 consumers, 3 partitions → one consumer sits idle
  Never add more consumers than partitions
  Want more parallelism? Increase partitions instead
```

![alt text](image-10.png)








![alt text](image-4.png)

`consumer_offsets` is topic created by kafka internally!! `which has all the information of consumer group read from which partition and which offset.



This is also why Kafka consumers are **stateless** — they don't need to remember where they were. Kafka remembers for them, per group, per partition.


![alt text](image-21.png)

now our consumer crashed and
a new consumer comes up and reading from same partition as before ,now where did it start?

so it do 

val=hash(group_id)% (Consumer_offsets.num_of_partitons)

this val is partititon number of consumer_offset group where it will go and see where it need to read.

Broker maintains a map of topic ,paritition combined as key and offset as value. so consumer get that offset from map and start reading from there  


![alt text](image-22.png)









![alt text](image-7.png)

Consumer also public offset it comiited to `__consumer_offsets` group.

 When a consumer reads an event, Kafka doesn't automatically mark it as "done" — you have to **commit** the offset to tell Kafka "I have successfully processed up to here." The strategy you choose decides **when** that commit happens.  

![alt text](image-2.png)

![alt text](image.png)

![alt text](image-3.png)

![alt text](image-5.png)


Kafka cluster is a group of Brokers working together to provide: 
 

- Scalability : distribute load across multiple servers 

- Fault tolerance: continue operation even if Brokers fails 

- High availability: No single point of failure. 

![alt text](image-6.png)

![alt text](image-12.png)

![alt text](image-8.png)

See for each partition we have one leader and other are followers.Let us see Replication factor

![alt text](image-13.png)

We need to know which broker hosts the partition leader

![alt text](image-14.png)
Followers never serve writes, Only serve reads

![alt text](image-11.png)







