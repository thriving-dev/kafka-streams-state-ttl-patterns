package dev.thriving.poc;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class ProcessorWithTTLCleanup extends ContextualProcessor<String, String, String, String> {

    private static Logger LOG = LoggerFactory.getLogger(ProcessorWithTTLCleanup.class);

    private static final String DELIMITER = "_";

    private KeyValueStore<String, String> store;
    private KeyValueStore<String, String> storeDeleteAt;

    @Override
    public void init(ProcessorContext<String, String> context) {
        super.init(context);
        store = context.getStateStore(KStreamsTopologyFactory.STATE_STORE);
        storeDeleteAt = context.getStateStore(KStreamsTopologyFactory.STATE_STORE_DELETION_IDX);

        context.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            LOG.info("@{} punctuator run for task: {}", timestamp, context.taskId());
            ArrayList<String> keysToRemove = new ArrayList<>();
            String yesterday = dayOfEpochMilli(timestamp - Duration.ofHours(24).toMillis());
            try (StringSerializer stringSerializer = new StringSerializer()) {
                try (KeyValueIterator<String, String> iter = storeDeleteAt.prefixScan(yesterday, stringSerializer)) {
                    iter.forEachRemaining(kv -> keysToRemove.add(kv.key));
                }
            }
            keysToRemove.forEach(timestampedKey -> {
                String key = timestampedKey.substring(timestampedKey.indexOf(DELIMITER) + 1);
                LOG.debug("[{}] evicting by idx: {} => key: {}", context.taskId(), timestampedKey, key);
                store.delete(key);
                storeDeleteAt.delete(timestampedKey);
            });

            ArrayList<String> knownKeys = new ArrayList<>();
            try (KeyValueIterator<String, String> iter = store.all()) {
                iter.forEachRemaining(kv -> {
                    knownKeys.add(kv.key);
                });
            }
            LOG.info("@{} [{}] punctuator run, known keys: {}", timestamp, context.taskId(), knownKeys);
        });
    }

    private static String dayOfEpochMilli(long timestamp) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneId.systemDefault());
        String day = formatter.format(instant);
        return day;
    }

    @Override
    public void process(Record<String, String> record) {
        if (record.value() == null) {
            LOG.debug("[{}] deleting record by key {}", context().taskId(), record.key());
            store.delete(record.key());
        } else {
            LOG.debug("[{}] persisting record {}:{}", context().taskId(), record.key(), record.value());
            store.put(record.key(), record.value());
            String day = dayOfEpochMilli(context().currentStreamTimeMs());
            String keyPrefixedwithTimestamp = day + DELIMITER + record.key();
            LOG.debug("[{}] persisting to delete-at store with key {}", context().taskId(), keyPrefixedwithTimestamp);
            storeDeleteAt.put(keyPrefixedwithTimestamp, ""); // write record with the key prefixed by timestamp + empty string value
        }
    }
}
