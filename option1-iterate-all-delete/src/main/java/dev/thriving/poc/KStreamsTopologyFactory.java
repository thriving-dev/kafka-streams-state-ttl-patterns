package dev.thriving.poc;

import dev.thriving.poc.avro.UserFlightBooking;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.Map;

@Factory
public class KStreamsTopologyFactory {

    private static final String INPUT_TOPIC = "user-flight-booking";
    static final String STATE_STORE = "store-with-ttl";

    @Singleton
    KStream<String, UserFlightBooking> exampleStream(ConfiguredStreamBuilder builder) {
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", "http://localhost:8081");

        Serde<String> stringSerde = Serdes.String();
        SpecificAvroSerde<UserFlightBooking>  userFlightBookingSerde = new SpecificAvroSerde<>();
        userFlightBookingSerde.configure(serdeConfig, false);

        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STATE_STORE),
                stringSerde,
                userFlightBookingSerde
        ));

        KStream<String, UserFlightBooking> stream = builder.stream(INPUT_TOPIC);

        stream.process(ProcessorWithTTLCleanup::new, STATE_STORE);

        return stream;
    }
}
