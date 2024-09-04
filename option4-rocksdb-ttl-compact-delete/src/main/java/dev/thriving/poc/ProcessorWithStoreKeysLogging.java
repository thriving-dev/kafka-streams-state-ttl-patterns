package dev.thriving.poc;

import dev.thriving.poc.avro.BaggageTracking;
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
import java.util.ArrayList;

@Slf4j
public class ProcessorWithStoreKeysLogging extends ContextualProcessor<String, BaggageTracking, String, BaggageTracking> {

    private KeyValueStore<String, BaggageTracking> store;

    @Override
    public void init(ProcessorContext<String, BaggageTracking> context) {
        super.init(context);
        store = context.getStateStore(KStreamsTopologyFactory.STATE_STORE);
        context.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            ArrayList<String> knownKeys = new ArrayList<>();
            try (KeyValueIterator<String, BaggageTracking> iter = store.all()) {
                iter.forEachRemaining(kv -> {
                    knownKeys.add(kv.key);
                });
            }
            log.info("@{} [{}] punctuator run, {} known keys: {}", timestamp, context.taskId(), knownKeys.size(), knownKeys);
        });
    }

    @Override
    public void process(Record<String, BaggageTracking> record) {
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
