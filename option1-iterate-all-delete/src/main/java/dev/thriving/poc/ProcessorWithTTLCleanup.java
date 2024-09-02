package dev.thriving.poc;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;

public class ProcessorWithTTLCleanup extends ContextualProcessor<String, String, String, String> {

    private static Logger LOG = LoggerFactory.getLogger(ProcessorWithTTLCleanup.class);

    private KeyValueStore<String, String> store;

    @Override
    public void init(ProcessorContext<String, String> context) {
        super.init(context);
        store = context.getStateStore(KStreamsTopologyFactory.STATE_STORE);
        context.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            LOG.info("@{} punctuator run for task: {}", timestamp, context.taskId());
            ArrayList<String> keysToRemove = new ArrayList<>();
            try (KeyValueIterator<String, String> iter = store.all()) {
                iter.forEachRemaining(kv -> {
                    if (kv.value.hashCode() % 2 == 0) {
                        keysToRemove.add(kv.key);
                    }
                });
            }
            keysToRemove.forEach(key -> {
                LOG.debug("evicting by key: {}", key);
                store.delete(key);
            });
        });
    }

    @Override
    public void process(Record<String, String> record) {
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
