package dev.thriving.poc;

import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Factory
public class KStreamsTopologyFactory {

    private static Logger LOG = LoggerFactory.getLogger(KStreamsTopologyFactory.class);

    private static final String INPUT_TOPIC = "option2-plaintext-input";
    static final String STATE_STORE = "some-store";

    @Singleton
    KStream<String, String> exampleStream(ConfiguredStreamBuilder builder) {
        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STATE_STORE),
                stringSerde,
                longSerde
        ));

        KTable<String, String> table = builder.table(INPUT_TOPIC);

        table.toStream()
                .process(ProcessorWithTTLCleanup::new, STATE_STORE)
                .to(INPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return table.toStream();
    }
}
