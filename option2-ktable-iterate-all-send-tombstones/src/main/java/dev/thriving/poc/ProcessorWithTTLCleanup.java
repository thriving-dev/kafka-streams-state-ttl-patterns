package dev.thriving.poc;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

public class ProcessorWithTTLCleanup extends ContextualProcessor<String, String, String, String> {

    private static Logger LOG = LoggerFactory.getLogger(ProcessorWithTTLCleanup.class);

    private KeyValueStore<String, Long> store;

    @Override
    public void init(ProcessorContext<String, String> context) {
        super.init(context);
        store = context.getStateStore(KStreamsTopologyFactory.STATE_STORE);
        context.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            LOG.info("@{} punctuator run for task: {}", timestamp, context.taskId());
            try (KeyValueIterator<String, Long> iter = store.all()) {
                iter.forEachRemaining(kv -> {
                    if (kv.value < timestamp - 30_000) {
                        LOG.debug("[{}] sending tombstone by key: {}", context.taskId(), kv.key);
                        context.forward(new Record<String, String>(kv.key, null, timestamp));
                    }
                });
            }
        });
    }

    @Override
    public void process(Record<String, String> record) {
        if (record.value() == null) {
            Long l = store.get(record.key());
            LOG.debug("[{}] deleting record by key {} - was {}", context().taskId(), record.key(), l);
            store.delete(record.key());
        } else {
            LOG.debug("persisting key:timestamp {}:{}", record.key(), context().currentStreamTimeMs());
            store.put(record.key(), context().currentStreamTimeMs());
        }
        try (KeyValueIterator<String, Long> iter = store.all()) {
            AtomicInteger i = new AtomicInteger();
            iter.forEachRemaining(kv -> i.getAndIncrement());
            LOG.debug("store size: {}", i.get());
        }
    }
}
