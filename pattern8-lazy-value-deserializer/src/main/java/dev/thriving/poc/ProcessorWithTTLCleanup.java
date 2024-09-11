package dev.thriving.poc;

import com.fasterxml.uuid.impl.UUIDUtil;
import dev.thriving.poc.avro.BaggageTracking;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.UUID;

public class ProcessorWithTTLCleanup extends ContextualProcessor<String, BaggageTracking, String, BaggageTracking> {

    private static Logger LOG = LoggerFactory.getLogger(ProcessorWithTTLCleanup.class);

    private KeyValueStore<String, LazySerde.Lazy<BaggageTracking>> store;

    @Override
    public void init(ProcessorContext<String, BaggageTracking> context) {
        super.init(context);
        store = context.getStateStore(KStreamsTopologyFactory.STATE_STORE);
        context.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            LOG.info("@{} punctuator run for task: {}", timestamp, context.taskId());
            ArrayList<String> keysToRemove = new ArrayList<>();
            try (KeyValueIterator<String, LazySerde.Lazy<BaggageTracking>> iter = store.all()) {
                iter.forEachRemaining(kv -> {
                    long deleteOlderThan = Instant.now().minusSeconds(60).toEpochMilli();
                    long timestampFromUUID7Key = UUIDUtil.extractTimestamp(UUIDUtil.uuid(kv.key));
                    if (timestampFromUUID7Key < deleteOlderThan) {
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
    public void process(Record<String, BaggageTracking> record) {
        if (record.value() == null) {
            LOG.debug("deleting record by key {}", record.key());
            store.delete(record.key());
        } else {
            LOG.debug("persisting record {}:{}", record.key(), record.value());
            store.put(record.key(), new LazySerde.Lazy<>(record.value()));
        }
        context().forward(record);
    }

}
