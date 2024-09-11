package dev.thriving.poc;

import dev.thriving.poc.avro.UserFlightBooking;
import lombok.extern.slf4j.Slf4j;
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
        context().forward(record);
    }

//    @Override
//    public void process(Record<String, UserFlightBooking> record) {   // basically the same as above...
//        store.put(record.key(), record.value());
//        context().forward(record);
//    }

}
