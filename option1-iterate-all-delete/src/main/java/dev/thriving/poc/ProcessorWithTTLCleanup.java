package dev.thriving.poc;

import dev.thriving.poc.avro.UserFlightBooking;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

@Slf4j
public class ProcessorWithTTLCleanup extends ContextualProcessor<String, UserFlightBooking, String, UserFlightBooking> {

    private KeyValueStore<String, UserFlightBooking> store;

    @Override
    public void init(ProcessorContext<String, UserFlightBooking> context) {
        super.init(context);
        store = context.getStateStore(KStreamsTopologyFactory.STATE_STORE);
        context.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
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
        });
    }

    @Override
    public void process(Record<String, UserFlightBooking> record) {
        if (record.value() == null) {
            log.debug("deleting record by key {}", record.key());
            store.delete(record.key());
        } else {
            log.debug("persisting record {}:{}", record.key(), record.value());
            store.put(record.key(), record.value());
        }
        context().forward(record);
    }

}
