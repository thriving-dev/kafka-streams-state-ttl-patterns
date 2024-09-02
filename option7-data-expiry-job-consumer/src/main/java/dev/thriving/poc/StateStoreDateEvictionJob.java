package dev.thriving.poc;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class StateStoreDateEvictionJob {

    private static Logger log = LoggerFactory.getLogger(StateStoreDateEvictionJob.class);

    public static void main(String[] args) {
        String topic = "option7-data-expiry-job-consumer-some-store-changelog"; // Replace with your topic name
        String destinationTopic = "option7-plaintext-input"; // Replace with your topic name

        // Configure Kafka Consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Replace with your Kafka broker
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "offset-checker-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // Configure Kafka Producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Replace with your Kafka broker
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");


        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
             KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            // Get partitions for the topic
            List<TopicPartition> partitions = new ArrayList<>( // we need a mutable list
                    consumer.partitionsFor(topic).stream()
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
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    // Log the record
                    log.debug("Consumed message: Key = {}, Value = {}, Partition = {}, Offset = {}",
                            record.key(), record.value(), record.partition(), record.offset());

                    if (record.value() != null && Long.parseLong(record.value()) % 2 == 0) {
                        log.debug("Forwarding tombstone: Key = {}", record.key());
                        // Option: depending on the topic, partitioning concept, you might want to set the partition, or partitioner to be used
                        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                                destinationTopic, record.key(), null);
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

    /**
     * Consume and log all messages from the beginning up to the recorded end offset for each partition.
     */
//    public static void main(String[] args) {
//        String topic = "option7-data-expiry-job-consumer-some-store-changelog"; // Replace with your topic name
//
//        // Configure Kafka Consumer
//        Properties props = new Properties();
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Replace with your Kafka broker
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, "offset-checker-group");
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
//
//        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
//            // Get partitions for the topic
//            List<TopicPartition> partitions = new ArrayList<>( // we need a mutable list
//                    consumer.partitionsFor(topic).stream()
//                            .map(info -> new TopicPartition(info.topic(), info.partition()))
//                            .toList()
//            );
//
//            // Assign partitions to the consumer
//            consumer.assign(partitions);
//
//            // Request end offsets
//            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
//
//            // Print the log end offsets for each partition
//            endOffsets.forEach((partition, offset) ->
//                    System.out.printf("Partition: %d, Log End Offset: %d%n", partition.partition(), offset));
//
//            // Seek to the beginning of each partition
//            consumer.seekToBeginning(partitions);
//
//            boolean consuming = true;
//
//            while (consuming) {
//                // Poll for records
//                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
//
//                for (ConsumerRecord<String, String> record : records) {
//                    // Log the record
//                    log.debug("Consumed message: Key = {}, Value = {}, Partition = {}, Offset = {}",
//                            record.key(), record.value(), record.partition(), record.offset());
//
//                    // Check if we've reached the end offset for the partition
//                    long endOffset = endOffsets.get(new TopicPartition(record.topic(), record.partition()));
//                    if (record.offset() >= endOffset - 1) {
//                        log.debug("Reached end offset for partition {}: {}", record.partition(), endOffset);
//                        partitions.remove(new TopicPartition(record.topic(), record.partition()));
//                    }
//                }
//
//                // Exit loop when topics contains no records
//                if (records.isEmpty()) {
//                    log.debug("No records received during poll, stopping consuming...");
//                    consuming = false;
//                }
//
//                // Exit loop when all partitions are fully consumed
//                if (partitions.isEmpty()) {
//                    log.debug("All partitions have been fully processed, stopping consuming...");
//                    consuming = false;
//                }
//            }
//
//            // Unsubscribe
//            consumer.unsubscribe();
//            log.debug("Finished");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

    /**
     * Read the log end offsets of a Kafka topic using the Java Kafka client.
     */
//    public static void main(String[] args) {
//        String topic = "option7-data-expiry-job-consumer-some-store-changelog"; // Replace with your topic name
//
//        // Configure Kafka Consumer
//        Properties props = new Properties();
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Replace with your Kafka broker
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, "offset-checker-group");
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
//
//        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
//            // Get partitions for the topic
//            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
//                    .map(info -> new TopicPartition(info.topic(), info.partition()))
//                    .toList();
//
//            // Assign partitions to the consumer
//            consumer.assign(partitions);
//
//            // Request end offsets
//            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
//
//            // Print the log end offsets for each partition
//            endOffsets.forEach((partition, offset) ->
//                    System.out.printf("Partition: %d, Log End Offset: %d%n", partition.partition(), offset));
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
}
