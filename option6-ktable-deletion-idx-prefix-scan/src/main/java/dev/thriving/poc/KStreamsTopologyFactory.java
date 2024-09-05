package dev.thriving.poc;

import dev.thriving.poc.avro.Flight;
import dev.thriving.poc.avro.FlightKey;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

@Factory
@Slf4j
public class KStreamsTopologyFactory {

    private static final String INPUT_TOPIC = "flight";
    static final String STATE_STORE = "flights";
    static final String STATE_STORE_DELETION_IDX = "flights-delete-at";

    @Singleton
    KStream<FlightKey, Flight> exampleStream(ConfiguredStreamBuilder builder) {
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", "http://localhost:8081");

        Serde<String> stringSerde = Serdes.String();
        SpecificAvroSerde<FlightKey> flightKeySerde = new SpecificAvroSerde<>();
        flightKeySerde.configure(serdeConfig, true);
        SpecificAvroSerde<Flight> flightSerde = new SpecificAvroSerde<>();
        flightSerde.configure(serdeConfig, false);

        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(STATE_STORE),
                flightKeySerde,
                flightSerde
        ));
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(STATE_STORE_DELETION_IDX),
                stringSerde,
                stringSerde
        ));

        KStream<FlightKey, Flight> stream = builder.stream(INPUT_TOPIC);

        stream.peek((k, v) -> log.info("peek {}:{}", k, v))
                .process(ProcessorWithTTLCleanup::new, STATE_STORE, STATE_STORE_DELETION_IDX);

        return stream;
    }
}
