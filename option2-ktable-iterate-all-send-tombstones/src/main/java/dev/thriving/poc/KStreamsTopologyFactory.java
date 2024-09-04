package dev.thriving.poc;

import dev.thriving.poc.avro.Flight;
import dev.thriving.poc.avro.FlightKey;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.Map;

@Factory
public class KStreamsTopologyFactory {

    private static final String INPUT_TOPIC = "flight";
    static final String STATE_STORE_TTL = "flight-departuretime-store";

    @Singleton
    KStream<FlightKey, Flight> exampleStream(ConfiguredStreamBuilder builder) {
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", "http://localhost:8081");

        Serde<Long> longSerde = Serdes.Long();
        SpecificAvroSerde<FlightKey> flightKeySerde = new SpecificAvroSerde<>();
        flightKeySerde.configure(serdeConfig, true);
        SpecificAvroSerde<Flight> flightSerde = new SpecificAvroSerde<>();
        flightSerde.configure(serdeConfig, false);

        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STATE_STORE_TTL),
                flightKeySerde,
                longSerde
        ));

        KTable<FlightKey, Flight> table = builder.table(INPUT_TOPIC);

        table.toStream()
                .process(ProcessorWithTTLCleanup::new, STATE_STORE_TTL)
                .to(INPUT_TOPIC, Produced.with(flightKeySerde, flightSerde));

        return table.toStream();
    }
}
