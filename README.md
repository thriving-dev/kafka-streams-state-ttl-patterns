# kafka-streams-state-ttl-patterns

Example implementations of different TTL Patterns for Kafka Streams.

Code from this repository was featured on [Current 2024](https://current.confluent.io/).

You can find slides and more information on: https://thriving.dev/talks/patterns-to-limit-kafka-streams-store-sizes-2024

### Local Playground

#### 1. Start docker compose stack
```bash
docker compose up -d
```

#### 2. Build gradle project
```bash
./gradlew clean build
```

#### 3. Produce test data
```bash
./gradlew common-datagen:produceBaggageTrackingEvents
./gradlew common-datagen:produceFlightEvents 
./gradlew common-datagen:produceUserFlightBookingEvents 
```

#### 4. Run individual projects
All patterns are built with micronaut, you can run them using your preferred IDE or also from command line:

```bash
./gradlew pattern1-iterate-all-delete:run
```

**Note:** There might be secondary tasks, e.g. for pattern7
```bash
./gradlew pattern7-data-expiry-job-consumer:runStateStoreDateEvictionJob
```

#### 5. Tear down docker compose stack
``` bash
docker compose down
```


### Talk Abstract
The delivery service startup OPS (Otter Parcel Service) is efficiently managing its fulfillment process.

A team of skilled otters had written a small but mighty fleet of Kafka Streams topologies, and soon OPS was serving its first customers. All data streams ran smoothly, and operators and clients tracked parcel movements in real-time. Everyone was happy, and OPS was on the road to success.

But with the business growing, the otters started to notice the applications getting more and more unwieldy to operate by the day, and infrastructure costs kept increasing. The experienced otter team also knew the reason for that. Their streams state is growing larger and larger over time. The very first fulfilled delivery, which was completed six months back and idling away, is still in the state stores.

Join the otters in tackling this tech debt story to
- understand the implications and causes for the deterioration of operations
- find a solution to keep only the data that is still needed (unfulfilled deliveries)
- learn about different patterns to purge or evict entries from state stores and KTables, such as TTL-based cleanup to expire data
- review, evaluate, and compare implementations

Are you curious if the team can solve their problem and which strategy will prevail? See you at my session, fellow otter 🦦!

(Although this story is a work of fiction, the use cases and solutions presented are based on real-world examples.)


### TODOs
- consider having all configuration inside KStreamsTopologyFactory
