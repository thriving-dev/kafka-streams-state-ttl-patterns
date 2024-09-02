package dev.thriving.poc;

import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Factory
public class KStreamsTopologyFactory {

    private static Logger LOG = LoggerFactory.getLogger(KStreamsTopologyFactory.class);

    private static final String INPUT_TOPIC = "option1-plaintext-input";
    static final String STATE_STORE = "some-store";

    @Singleton
    KStream<String, String> exampleStream(ConfiguredStreamBuilder builder) {
        Serde<String> stringSerde = Serdes.String();
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STATE_STORE),
                stringSerde,
                stringSerde
        ));

        KStream<String, String> stream = builder.<String, String>stream(INPUT_TOPIC);

        stream.peek((k, v) -> LOG.info("peek {}:{}", k, v))
                .process(ProcessorWithTTLCleanup::new, STATE_STORE);

        return stream;
    }
}
