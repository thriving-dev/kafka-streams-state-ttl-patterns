package dev.thriving.poc;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedEpochGenerator;
import dev.thriving.poc.avro.BaggageStatus;
import dev.thriving.poc.avro.BaggageTracking;
import dev.thriving.poc.utils.AdjustableUUIDClock;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class BaggageTrackingProducer {

    public static void main(String[] args) {
        String topicName = "baggage-tracking";
        String schemaRegistryUrl = "http://localhost:8081";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        // Initialize the producer
        Producer<String, BaggageTracking> producer = new KafkaProducer<>(props);

        // Clock for generating UUID v7
        AdjustableUUIDClock uuidClock = new AdjustableUUIDClock();
        uuidClock.shiftTime(Duration.ofMinutes(-1));
        TimeBasedEpochGenerator uuid7Generator = Generators.timeBasedEpochGenerator(null, uuidClock);

        // Produce 10 records
        for (int i = 0; i < 30; i++) {
            // Create a BaggageTracking record
            BaggageTracking record = createTestBaggageTracking(uuid7Generator, i);

            // shift time
            uuidClock.shiftTime(Duration.ofSeconds(6));

            // Create a producer record with the key and value
            ProducerRecord<String, BaggageTracking> producerRecord = new ProducerRecord<>(
                    topicName,
                    null,
                    uuidClock.currentTimeMillis(),
                    record.getTrackingId(),
                    record);

            // Send the record to Kafka
            Future<RecordMetadata> future = producer.send(producerRecord);
            try {
                RecordMetadata metadata = future.get();
                System.out.printf("Produced record to topic %s partition [%d] @ offset %d%n",
                        metadata.topic(), metadata.partition(), metadata.offset());
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        // Close the producer
        producer.close();
    }

    private static BaggageTracking createTestBaggageTracking(TimeBasedEpochGenerator uuid7Generator, int i) {
        return BaggageTracking.newBuilder()
                .setTrackingId(uuid7Generator.generate().toString())
                .setBaggageId("baggage-" + i)
                .setFlightNumber("AA" + (100 + i))
                .setDepartureDate("2024-09-04")
                .setScanTime("2024-09-04T10:15:30Z")
                .setAirportCode("JFK")
                .setLocation("T1")
                .setStatus(BaggageStatus.LOADED_ON_FLIGHT)
                .build();
    }
}
