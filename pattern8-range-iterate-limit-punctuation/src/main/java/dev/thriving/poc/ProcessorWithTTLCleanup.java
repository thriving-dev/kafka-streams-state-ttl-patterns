package dev.thriving.poc;

import dev.thriving.poc.avro.UserFlightBooking;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

@Slf4j
public class ProcessorWithTTLCleanup extends ContextualProcessor<String, UserFlightBooking, String, UserFlightBooking> {

    private KeyValueStore<String, UserFlightBooking> store;

    @Override
    public void init(ProcessorContext<String, UserFlightBooking> context) {
        super.init(context);
        store = context.getStateStore(KStreamsTopologyFactory.STATE_STORE);
        context.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, new StoreTTLPunctuator(context, store));
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
        safeForward(context(), record);
    }

//    @Override
//    // PS: this behaves the same as above, deleting for null values...
//    public void process(Record<String, UserFlightBooking> record) {
//        store.put(record.key(), record.value());
//        context().forward(record);
//    }


    public static <K, V> void safeForward(ProcessorContext<K, V> context, Record<K, V> record) {
        // Defensive copy of headers to avoid shared references and side effects
        Headers newHeaders = new RecordHeaders(record.headers());

        // Create a new Record with the copied headers and forward it
        Record<K, V> newRecord = new Record<>(record.key(), record.value(), record.timestamp(), newHeaders);
        context.forward(newRecord);
    }
}
