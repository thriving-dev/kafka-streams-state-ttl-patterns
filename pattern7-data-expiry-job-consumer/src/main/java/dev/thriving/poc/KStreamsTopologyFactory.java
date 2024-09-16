package dev.thriving.poc;

import dev.thriving.poc.avro.Flight;
import dev.thriving.poc.avro.FlightKey;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Collections;
import java.util.Map;

@Slf4j
@Factory
public class KStreamsTopologyFactory {

    private static final String INPUT_TOPIC = "flight";
    static final String STATE_STORE = "flights";

    @Singleton
    KStream<FlightKey, Flight> exampleStream(ConfiguredStreamBuilder builder) {
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", "http://localhost:8081");

        SpecificAvroSerde<FlightKey> flightKeySerde = new SpecificAvroSerde<>();
        flightKeySerde.configure(serdeConfig, true);
        SpecificAvroSerde<Flight> flightSerde = new SpecificAvroSerde<>();
        flightSerde.configure(serdeConfig, false);

        KStream<FlightKey, Flight> stream = builder.stream(INPUT_TOPIC);

        stream.peek((k, v) -> log.info("peek {}:{}", k, v))
                .toTable(Named.as("flights"),
                        Materialized.<FlightKey, Flight, KeyValueStore<Bytes, byte[]>>as(STATE_STORE)
                                .withKeySerde(flightKeySerde).withValueSerde(flightSerde)
                );

        return stream;
    }
}
