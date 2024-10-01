package dev.thriving.poc;

import dev.thriving.poc.avro.Flight;
import dev.thriving.poc.avro.FlightKey;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.Topic;

import static dev.thriving.poc.KStreamsTopologyFactory.INPUT_TOPIC;

@KafkaClient(id = "state-store-cleanup-job", acks = KafkaClient.Acknowledge.ALL)
public interface KafkaProducer {

    @Topic(INPUT_TOPIC)
    void sendFlight(@KafkaKey FlightKey key, Flight value);

}
