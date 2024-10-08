package dev.thriving.poc;

import dev.thriving.poc.avro.BaggageTracking;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.Map;

@Slf4j
@Factory
public class KStreamsTopologyFactory {

    private static final String INPUT_TOPIC = "baggage-tracking";
    static final String STATE_STORE = "tracking-lru";

    @Singleton
    KStream<String, BaggageTracking> exampleStream(ConfiguredStreamBuilder builder) {
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", "http://localhost:8081");

        Serde<String> stringSerde = Serdes.String();
        SpecificAvroSerde<BaggageTracking> baggageTrackingSerde = new SpecificAvroSerde<>();
        baggageTrackingSerde.configure(serdeConfig, false);

        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.lruMap(STATE_STORE, 5),
                stringSerde,
                baggageTrackingSerde
        ).withLoggingEnabled(Map.of(
                TopicConfig.CLEANUP_POLICY_CONFIG, "compact,delete",
                TopicConfig.RETENTION_MS_CONFIG, "300000", // 5m
                TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, "300000" // 5m
        )));

        KStream<String, BaggageTracking> stream = builder.stream(INPUT_TOPIC);

        stream.peek((k, v) -> log.info("peek {}:{}", k, v))
                .process(PersistToStoreProcessor::new, STATE_STORE);

        return stream;
    }
}
