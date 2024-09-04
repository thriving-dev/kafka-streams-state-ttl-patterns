package dev.thriving.poc;

import dev.thriving.poc.avro.Flight;
import dev.thriving.poc.avro.FlightKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ProcessorWithTTLCleanup extends ContextualProcessor<FlightKey, Flight, FlightKey, Flight> {

    private KeyValueStore<FlightKey, Long> store;

    @Override
    public void init(ProcessorContext<FlightKey, Flight> context) {
        super.init(context);
        store = context.getStateStore(KStreamsTopologyFactory.STATE_STORE_TTL);
        context.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            log.info("@{} punctuator run for task: {}", timestamp, context.taskId());
            try (KeyValueIterator<FlightKey, Long> iter = store.all()) {
                iter.forEachRemaining(kv -> {
                    if (kv.value < timestamp - 30_000) {
                        log.debug("[{}] sending tombstone by key: {}", context.taskId(), kv.key);
                        context.forward(new Record<>(kv.key, null, timestamp));
                    }
                });
            }
        });
    }

    @Override
    public void process(Record<FlightKey, Flight> record) {
        if (record.value() == null) {
            log.debug("[{}] deleting record by key {}", context().taskId(), record.key());
            store.delete(record.key());
        } else {
            Long departureTimeMillis = parseToEpochMilli(record.value().getDepartureTime());
            log.debug("persisting key:timestamp {}:{}", record.key(), departureTimeMillis);
            store.put(record.key(), departureTimeMillis);
        }
        try (KeyValueIterator<FlightKey, Long> iter = store.all()) {
            AtomicInteger i = new AtomicInteger();
            iter.forEachRemaining(kv -> i.getAndIncrement());
            log.debug("store size: {}", i.get());
        }
    }

    private static long parseToEpochMilli(String departureTime) {
        return LocalDateTime.parse(departureTime, DateTimeFormatter.ISO_DATE_TIME).toInstant(ZoneOffset.UTC).toEpochMilli();
    }
}
