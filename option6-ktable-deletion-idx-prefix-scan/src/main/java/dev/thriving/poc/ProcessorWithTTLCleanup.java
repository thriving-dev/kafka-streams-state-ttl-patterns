package dev.thriving.poc;

import dev.thriving.poc.avro.Flight;
import dev.thriving.poc.avro.FlightKey;
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
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class ProcessorWithTTLCleanup extends ContextualProcessor<FlightKey, Flight, FlightKey, Flight> {

    private static Logger LOG = LoggerFactory.getLogger(ProcessorWithTTLCleanup.class);

    private static final String DELIMITER = "_";

    private KeyValueStore<FlightKey, Flight> store;
    private KeyValueStore<String, String> storeDeleteAt;

    @Override
    public void init(ProcessorContext<FlightKey, Flight> context) {
        super.init(context);
        store = context.getStateStore(KStreamsTopologyFactory.STATE_STORE);
        storeDeleteAt = context.getStateStore(KStreamsTopologyFactory.STATE_STORE_DELETION_IDX);

        context.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            LOG.info("@{} punctuator run for task: {}", timestamp, context.taskId());
            ArrayList<String> keysToRemove = new ArrayList<>();
            String expireFlightsWithDepartureDate2dAgo = DateTimeFormatter.ISO_LOCAL_DATE.format(
                    Instant.ofEpochMilli(timestamp - Duration.ofHours(48).toMillis()).atZone(ZoneOffset.UTC));
            try (StringSerializer stringSerializer = new StringSerializer()) {
                try (KeyValueIterator<String, String> iter = storeDeleteAt.prefixScan(expireFlightsWithDepartureDate2dAgo, stringSerializer)) {
                    iter.forEachRemaining(kv -> keysToRemove.add(kv.key));
                }
            }
            keysToRemove.forEach(timestampedKey -> {
                FlightKey key = fromStringKeyPrefixedWithDate(timestampedKey);
                LOG.debug("[{}] evicting by idx: {} => key: {}", context.taskId(), timestampedKey, key);
                store.delete(key);
                storeDeleteAt.delete(timestampedKey);
            });

            ArrayList<Object> knownKeys = new ArrayList<>();
            try (KeyValueIterator<FlightKey, Flight> iter = store.all()) {
                iter.forEachRemaining(kv -> {
                    knownKeys.add(kv.key);
                });
            }
            LOG.info("@{} [{}] punctuator run, {} known keys: {}", timestamp, context.taskId(), knownKeys.size(), knownKeys);
        });
    }

    @Override
    public void process(Record<FlightKey, Flight> record) {
        if (record.value() == null) {
            LOG.debug("[{}] deleting record by key {}", context().taskId(), record.key());
            store.delete(record.key());
        } else {
            LOG.debug("[{}] persisting record {}:{}", context().taskId(), record.key(), record.value());
            store.put(record.key(), record.value());
            String stringKeyPrefixedWithDate = toStringKeyPrefixedWithDate(record.key());
            LOG.debug("[{}] persisting to delete-at store with key {}", context().taskId(), stringKeyPrefixedWithDate);
            storeDeleteAt.put(stringKeyPrefixedWithDate, ""); // write record with the key prefixed by timestamp + empty string value
        }
    }

    private static String toStringKeyPrefixedWithDate(FlightKey key) {
        return key.getDepartureDate() + DELIMITER + key.getFlightNumber();
    }

    private static FlightKey fromStringKeyPrefixedWithDate(String key) {
        String[] parts = key.split(DELIMITER);
        return FlightKey.newBuilder().setDepartureDate(parts[0]).setFlightNumber(parts[1]).build();
    }
}
