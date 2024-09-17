package dev.thriving.poc;

import dev.thriving.poc.avro.UserFlightBooking;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

@Slf4j
public class StoreTTLPunctuator implements Punctuator {

    private final ProcessorContext<String, UserFlightBooking> context;
    private final KeyValueStore<String, UserFlightBooking> store;

    public StoreTTLPunctuator(ProcessorContext<String, UserFlightBooking> context,
                              KeyValueStore<String, UserFlightBooking> store) {
        this.context = context;
        this.store = store;
    }

    @Override
    public void punctuate(long timestamp) {
        log.info("@{} punctuator run for task: {}", timestamp, context.taskId());
        ArrayList<String> keysToRemove = new ArrayList<>();
        try (KeyValueIterator<String, UserFlightBooking> iter = store.all()) {
            iter.forEachRemaining(kv -> {
                LocalDate departureDate = LocalDate.parse(kv.value.getDepartureDate(), DateTimeFormatter.ISO_LOCAL_DATE);
                // delete records 3 days after departureDate
                if (departureDate.isBefore(LocalDate.now().minusDays(3))) {
                    keysToRemove.add(kv.key);
                }
            });
        }
        keysToRemove.forEach(key -> {
            log.debug("evicting by key: {}", key);
            store.delete(key);
        });
    }
}
