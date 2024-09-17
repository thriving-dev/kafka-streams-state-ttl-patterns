package dev.thriving.poc;

import dev.thriving.poc.avro.Flight;
import dev.thriving.poc.avro.FlightKey;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class StateStoreDataEvictionJob {

    private static String CHANGELOG_TOPIC = "pattern7-data-expiry-job-consumer-flights-changelog"; // Replace with your topic name
    private static String ORIGINAL_TOPIC = "flight"; // Replace with your topic name

    public static void main(String[] args) {
        try (KafkaConsumer<FlightKey, Flight> consumer = new KafkaConsumer<>(consumerProps());
             KafkaProducer<FlightKey, Flight> producer = new KafkaProducer<>(producerProps())) {
            // Get partitions for the topic
            List<TopicPartition> partitions = new ArrayList<>( // we need a mutable list
                    consumer.partitionsFor(CHANGELOG_TOPIC).stream()
                            .map(info -> new TopicPartition(info.topic(), info.partition()))
                            .toList()
            );

            // Assign partitions to the consumer
            consumer.assign(partitions);

            // Request end offsets
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

            // Print the log end offsets for each partition
            endOffsets.forEach((partition, offset) ->
                    System.out.printf("Partition: %d, Log End Offset: %d%n", partition.partition(), offset));

            // Seek to the beginning of each partition
            consumer.seekToBeginning(partitions);

            boolean consuming = true;

            while (consuming) {
                // Poll for records
                ConsumerRecords<FlightKey, Flight> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<FlightKey, Flight> record : records) {
                    // Log the record
                    log.debug("Consumed message: Key = {}, Value = {}, Partition = {}, Offset = {}",
                            record.key(), record.value(), record.partition(), record.offset());

                    if (record.value() != null && hasExpired(record)) {
                        log.debug("Flight arrivalTime is 6h in the past, forwarding tombstone: Key = {}", record.key());
                        // Option: depending on the topic, partitioning concept, you might want to set the partition, or partitioner to be used
                        ProducerRecord<FlightKey, Flight> producerRecord = new ProducerRecord<>(
                                ORIGINAL_TOPIC, record.key(), null);
                        producer.send(producerRecord);
                    }

                    // Check if we've reached the end offset for the partition
                    long endOffset = endOffsets.get(new TopicPartition(record.topic(), record.partition()));
                    if (record.offset() >= endOffset - 1) {
                        log.debug("Reached end offset for partition {}: {}", record.partition(), endOffset);
                        partitions.remove(new TopicPartition(record.topic(), record.partition()));
                    }
                }

                // Exit loop when topics contains no records
                if (records.isEmpty()) {
                    log.debug("No records received during poll, stopping consuming...");
                    consuming = false;
                }

                // Exit loop when all partitions are fully consumed
                if (partitions.isEmpty()) {
                    log.debug("All partitions have been fully processed, stopping consuming...");
                    consuming = false;
                }
            }

            // Unsubscribe
            consumer.unsubscribe();
            log.debug("Finished");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static boolean hasExpired(ConsumerRecord<FlightKey, Flight> record) {
        long expireFlightsArrivedBefore = Instant.now().minus(Duration.ofHours(12)).toEpochMilli();
        return parseToEpochMilli(record.value().getArrivalTime()) < expireFlightsArrivedBefore;
    }

    private static Properties producerProps() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Replace with your Kafka broker
        producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081"); // Replace with your Kafka broker
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "state-store-eviction-tombstone-producer");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer");
        return producerProps;
    }

    private static Properties consumerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Replace with your Kafka broker
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081"); // Replace with your Schema registry
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "offset-checker-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return props;
    }

    private static long parseToEpochMilli(String departureTime) {
        return LocalDateTime.parse(departureTime, DateTimeFormatter.ISO_DATE_TIME).toInstant(ZoneOffset.UTC).toEpochMilli();
    }
}
