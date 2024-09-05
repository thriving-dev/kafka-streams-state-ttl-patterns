package dev.thriving.poc;

import dev.thriving.poc.avro.UserFlightBooking;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

@Slf4j
public class ProcessorWithTTLCleanup extends ContextualProcessor<String, UserFlightBooking, String, UserFlightBooking> {

    private static final String DELIMITER = "_";

    private KeyValueStore<String, UserFlightBooking> store;
    private KeyValueStore<String, String> storeDeleteAt;

    @Override
    public void init(ProcessorContext<String, UserFlightBooking> context) {
        super.init(context);
        store = context.getStateStore(KStreamsTopologyFactory.STATE_STORE);
        storeDeleteAt = context.getStateStore(KStreamsTopologyFactory.STATE_STORE_DELETION_IDX);

        context.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            log.info("@{} punctuator run for task: {}", timestamp, context.taskId());
            ArrayList<String> keysToRemove = new ArrayList<>();
            try (KeyValueIterator<String, String> iter = storeDeleteAt.range(null, Long.toString(timestamp ))) {
                iter.forEachRemaining(kv -> keysToRemove.add(kv.key));
            }
            keysToRemove.forEach(timestampedKey -> {
                String key = timestampedKey.substring(timestampedKey.indexOf(DELIMITER) + 1);
                log.debug("[{}] evicting by idx: {} => key: {}", context.taskId(), timestampedKey, key);
                store.delete(key);
                storeDeleteAt.delete(timestampedKey);
            });

            ArrayList<String> knownKeys = new ArrayList<>();
            try (KeyValueIterator<String, UserFlightBooking> iter = store.all()) {
                iter.forEachRemaining(kv -> {
                    knownKeys.add(kv.key);
                });
            }
            log.info("@{} [{}] punctuator run, {} known keys: {}", timestamp, context.taskId(), knownKeys.size(), knownKeys);
        });
    }

    @Override
    public void process(Record<String, UserFlightBooking> record) {
        if (record.value() == null) {
            log.debug("[{}] deleting record by key {}", context().taskId(), record.key());
            store.delete(record.key());
        } else {
            log.debug("[{}] persisting record {}:{}", context().taskId(), record.key(), record.value());
            store.put(record.key(), record.value());

            LocalDate departureDate = LocalDate.parse(record.value().getDepartureDate(), DateTimeFormatter.ISO_LOCAL_DATE);
            Instant departureDateAtStartOfDay = departureDate.atStartOfDay(ZoneOffset.UTC).toInstant();

            // TTL: evict from store at midnight UTC, two days after the departure date
            long twoDaysAfterDepartureDateEpochMilli = departureDateAtStartOfDay.plus(Duration.ofDays(2)).toEpochMilli();

            String keyPrefixedwithTimestamp = twoDaysAfterDepartureDateEpochMilli + DELIMITER + record.key();
            log.debug("[{}] persisting to 'delete-at' store with key {}", context().taskId(), keyPrefixedwithTimestamp);

            // write record with the key prefixed by timestamp + empty string value. (value is never used)
            storeDeleteAt.put(keyPrefixedwithTimestamp, "");
        }
    }
}
