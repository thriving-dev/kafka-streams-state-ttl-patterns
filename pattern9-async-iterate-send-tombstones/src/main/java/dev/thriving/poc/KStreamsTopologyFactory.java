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
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Collections;
import java.util.Map;

@Factory
@Slf4j
public class KStreamsTopologyFactory {

    static final String INPUT_TOPIC = "flight";
    static final String STATE_STORE_FLIGHTS = "flights";

    @Singleton
    KStream<FlightKey, Flight> exampleStream(ConfiguredStreamBuilder builder) {
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", "http://localhost:8081");

        SpecificAvroSerde<FlightKey> flightKeySerde = new SpecificAvroSerde<>();
        flightKeySerde.configure(serdeConfig, true);
        SpecificAvroSerde<Flight> flightSerde = new SpecificAvroSerde<>();
        flightSerde.configure(serdeConfig, false);

        Materialized<FlightKey, Flight, KeyValueStore<Bytes, byte[]>> materialized = Materialized.as(STATE_STORE_FLIGHTS);
        materialized.withCachingDisabled(); // for demo purpose, disable 'caching'

        KTable<FlightKey, Flight> table = builder.table(INPUT_TOPIC, materialized);

        return table.toStream()
                .peek((k, v) -> log.info("peek {}:{}", k, v));
    }
}
