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
import java.util.ArrayList;

public class MyProcessor extends ContextualProcessor<String, String, String, String> {

    private static Logger LOG = LoggerFactory.getLogger(MyProcessor.class);

    private KeyValueStore<String, String> store;

    @Override
    public void init(ProcessorContext<String, String> context) {
        super.init(context);
        store = context.getStateStore(KStreamsTopologyFactory.STATE_STORE);
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
