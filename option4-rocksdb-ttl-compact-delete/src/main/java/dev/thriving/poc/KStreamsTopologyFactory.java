package dev.thriving.poc;

import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

@Slf4j
@Factory
public class KStreamsTopologyFactory {

    private static final String INPUT_TOPIC = "option4-plaintext-input";
    static final String STATE_STORE = "some-store";

    @Singleton
    KStream<String, String> exampleStream(ConfiguredStreamBuilder builder) {
        Serde<String> stringSerde = Serdes.String();
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STATE_STORE),
                stringSerde,
                stringSerde
        ).withLoggingEnabled(
                Map.of(
                        TopicConfig.CLEANUP_POLICY_CONFIG, "compact,delete",
                        TopicConfig.RETENTION_MS_CONFIG, "60000", // 1m
                        TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, "60000" // 1m
//                        TopicConfig.RETENTION_MS_CONFIG, "172800000" // 48h
//                        TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, "172800000" // 48h
                )
        ));

        KStream<String, String> stream = builder.<String, String>stream(INPUT_TOPIC);

        stream.peek((k, v) -> log.info("peek {}:{}", k, v))
                .process(ProcessorWithStoreKeysLogging::new, STATE_STORE);

        return stream;
    }
}
