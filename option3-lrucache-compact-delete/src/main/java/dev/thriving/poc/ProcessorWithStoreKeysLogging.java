package dev.thriving.poc;

import dev.thriving.poc.avro.BaggageTracking;
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

public class ProcessorWithStoreKeysLogging extends ContextualProcessor<String, BaggageTracking, String, BaggageTracking> {

    private static Logger LOG = LoggerFactory.getLogger(ProcessorWithStoreKeysLogging.class);

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
            LOG.info("@{} [{}] punctuator run, {} known keys: {}", timestamp, context.taskId(), knownKeys.size(), knownKeys);
        });
    }

    @Override
    public void process(Record<String, BaggageTracking> record) {
        if (record.value() == null) {
            LOG.debug("deleting record by key {}", record.key());
            store.delete(record.key());
        } else {
            LOG.debug("persisting record {}:{}", record.key(), record.value());
            store.put(record.key(), record.value());
        }
        context().forward(record);
    }

}
