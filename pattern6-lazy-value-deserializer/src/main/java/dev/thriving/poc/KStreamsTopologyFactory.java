package dev.thriving.poc;

import dev.thriving.poc.avro.BaggageTracking;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.Stores;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Factory
public class KStreamsTopologyFactory {

    private static final String INPUT_TOPIC = "baggage-tracking";
    static final String STATE_STORE = "baggage-tracking";

    @Singleton
    KStream<String, BaggageTracking> exampleStream(ConfiguredStreamBuilder builder) {
        Map<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put("schema.registry.url", "http://localhost:8081");

        Serde<String> stringSerde = Serdes.String();
        SpecificAvroSerde<BaggageTracking>  baggageTrackingSerde = new SpecificAvroSerde<>();
        baggageTrackingSerde.configure(serdeConfig, false);
        LazySerde<BaggageTracking>  lazyBaggageTrackingSerde = new LazySerde<>(baggageTrackingSerde);

        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(STATE_STORE),
                stringSerde,
                lazyBaggageTrackingSerde
        ));

        KStream<String, BaggageTracking> stream = builder.stream(INPUT_TOPIC, Consumed.with(stringSerde, baggageTrackingSerde));

        stream.peek((k, v) -> log.info("peek {}:{}", k, v))
                .process(ProcessorWithTTLCleanup::new, STATE_STORE);

        return stream;
    }
}
