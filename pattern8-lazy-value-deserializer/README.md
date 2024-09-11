# pattern8-lazy-value-deserializer

echo "$(uuid7):$(echo '{"trackingId": "123e4567-e89b-12d3-a456-426614174000", "baggageId": "123e4567-e89b-12d3-a456-426614174001", "flightNumber": "AA123", "departureDate": "2024-09-04", "scanTime": "2024-09-04T10:15:30Z", "airportCode": "JFK", "location": "LOADING", "status": "LOADED_ON_FLIGHT"}' | kcat -b localhost:9092 -r http://localhost:8081 -t passenger-baggage-tracking -s value=avro -r value=../avros/src/main/avro/BaggageTracking.avsc)" | kcat -P -b localhost:9092 -t passenger-baggage-tracking -K:

docker run -it --rm confluentinc/cp-schema-registry:7.7.0 kafka-avro-console-producer

## Micronaut 4.6.0 Documentation

- [User Guide](https://docs.micronaut.io/4.6.0/guide/index.html)
- [API Reference](https://docs.micronaut.io/4.6.0/api/index.html)
- [Configuration Reference](https://docs.micronaut.io/4.6.0/guide/configurationreference.html)
- [Micronaut Guides](https://guides.micronaut.io/index.html)
---

- [Micronaut Gradle Plugin documentation](https://micronaut-projects.github.io/micronaut-gradle-plugin/latest/)
- [GraalVM Gradle Plugin documentation](https://graalvm.github.io/native-build-tools/latest/gradle-plugin.html)
- [Shadow Gradle Plugin](https://plugins.gradle.org/plugin/com.github.johnrengelman.shadow)
## Feature kafka documentation

- [Micronaut Kafka Messaging documentation](https://micronaut-projects.github.io/micronaut-kafka/latest/guide/index.html)


## Feature micronaut-aot documentation

- [Micronaut AOT documentation](https://micronaut-projects.github.io/micronaut-aot/latest/guide/)


## Feature serialization-jackson documentation

- [Micronaut Serialization Jackson Core documentation](https://micronaut-projects.github.io/micronaut-serialization/latest/guide/)


## Feature kafka-streams documentation

- [Micronaut Kafka Streams documentation](https://micronaut-projects.github.io/micronaut-kafka/latest/guide/index.html#kafkaStream)


## Feature test-resources documentation

- [Micronaut Test Resources documentation](https://micronaut-projects.github.io/micronaut-test-resources/latest/guide/)


