package dev.thriving.poc;

import dev.thriving.poc.avro.UserFlightBooking;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

@Slf4j
public class StoreTTLPunctuator implements Punctuator {

    private static final long MAX_PUNCTUATION_DURATION_MS = 3_000;

    private final ProcessorContext<String, UserFlightBooking> context;
    private final KeyValueStore<String, UserFlightBooking> store;

    private String lastProcessedKey = null;

    public StoreTTLPunctuator(ProcessorContext<String, UserFlightBooking> context,
                              KeyValueStore<String, UserFlightBooking> store) {
        this.context = context;
        this.store = store;
    }

    @SneakyThrows
    @Override
    public void punctuate(long timestamp) {
        log.info("[{}@{}] punctuator run", context.taskId(), timestamp);
        long startedAt = System.currentTimeMillis();
        store.flush(); // flush first, to allow deletes while iterating

        // range query and iterate store entries, continue 'from' where we left off (or NULL := beginning)
        log.info("[{}@{}] range({}, null)", context.taskId(), timestamp, lastProcessedKey);
        try (KeyValueIterator<String, UserFlightBooking> iter = store.range(lastProcessedKey, null)) {
            while (iter.hasNext()) {
                KeyValue<String, UserFlightBooking> kv = iter.next();
                log.info("[{}@{}] processing key {}", context.taskId(), timestamp, kv.key);
                if (hasExpired(kv)) {
                    store.delete(kv.key);
                }
                Thread.sleep(1000); // throttle, to simulate behaviour
                long now = System.currentTimeMillis();
                if (startedAt + MAX_PUNCTUATION_DURATION_MS < now) {
                    log.info("[{}@{}] punctuator run breached MAX_PUNCTUATION_DURATION_MS={}, interrupt and remember key {} to continue on next punctuate", context.taskId(), now, MAX_PUNCTUATION_DURATION_MS, kv.key);
                    lastProcessedKey = kv.key;
                    break;
                }
            }
            if (!iter.hasNext()) {
                lastProcessedKey = null;
            }
        }
    }

    private static boolean hasExpired(KeyValue<String, UserFlightBooking> kv) {
        LocalDate departureDate = LocalDate.parse(kv.value.getDepartureDate(), DateTimeFormatter.ISO_LOCAL_DATE);
        // delete records 3 days after departureDate
        boolean hasExpired = departureDate.isBefore(LocalDate.now().minusDays(3));
        return hasExpired;
    }
}
