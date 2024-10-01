package dev.thriving.poc;

import dev.thriving.poc.avro.Flight;
import dev.thriving.poc.avro.FlightKey;
import io.micronaut.context.annotation.Factory;
import io.micronaut.scheduling.annotation.Scheduled;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Slf4j
@Factory
public class AsyncCleanUpJob {

    private static final long TTL_MILLIS = 30_000;

    @Scheduled(initialDelay = "10s", fixedDelay = "10s")
    public void cleanup(KafkaStreams streams, KafkaProducer producer) {
        long timestamp = System.currentTimeMillis();
        log.info("@{} scheduled job run", timestamp);

        if (streams.state() != KafkaStreams.State.RUNNING) {
            log.info("@{} skipping run, streams is not ready: {}", timestamp, streams.state());
            return;
        }

        ReadOnlyKeyValueStore<FlightKey, Flight> store = streams.store(StoreQueryParameters.fromNameAndType(KStreamsTopologyFactory.STATE_STORE_FLIGHTS, QueryableStoreTypes.keyValueStore()));
        try (KeyValueIterator<FlightKey, Flight> iter = store.all()) {
            iter.forEachRemaining(kv -> {
                if (parseToEpochMilli(kv.value.getDepartureTime()) < timestamp - TTL_MILLIS) {
                    log.debug("Sending tombstone by key: {}", kv.key);
                    producer.sendFlight(kv.key, null);
                }
            });
        }
    }

    private long parseToEpochMilli(String departureTime) {
        return LocalDateTime.parse(departureTime, DateTimeFormatter.ISO_DATE_TIME).toInstant(ZoneOffset.UTC).toEpochMilli();
    }
}
