package dev.thriving.poc;

import dev.thriving.poc.avro.Flight;
import dev.thriving.poc.avro.FlightCategory;
import dev.thriving.poc.avro.FlightKey;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class FlightProducer {

    public static void main(String[] args) {
        String topicName = "flight";
        String schemaRegistryUrl = "http://localhost:8081";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", KafkaAvroSerializer.class.getName());
        props.put("value.serializer", KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        // Initialize the producer
        Producer<FlightKey, Flight> producer = new KafkaProducer<>(props);

        LocalDateTime startDateTime = LocalDateTime.now().minusDays(14); // Start 14 days in the past

        // Produce 10 records
        for (int i = 0; i < 30; i++) {
            // Create a BaggageTracking record
            String flightNumber = "FL" + (1000 + new Random().nextInt(9000));
            String departureDate = startDateTime.plusDays(i).format(DateTimeFormatter.ISO_LOCAL_DATE);
            LocalDateTime departureTime = startDateTime.plusDays(i).plusHours(6);
            LocalDateTime arrivalTime = departureTime.plusHours(2);

            FlightKey key = FlightKey.newBuilder()
                    .setFlightNumber(flightNumber)
                    .setDepartureDate(departureDate)
                    .build();

            Flight record = createTestFlight(flightNumber,
                    departureTime.format(DateTimeFormatter.ISO_DATE_TIME),
                    arrivalTime.format(DateTimeFormatter.ISO_DATE_TIME),
                    i);

            // Create a producer record with the key and value
            ProducerRecord<FlightKey, Flight> producerRecord = new ProducerRecord<>(
                    topicName,
                    null,
                    startDateTime.plusDays(i - 2).toInstant(ZoneOffset.UTC).toEpochMilli(),
                    key,
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

    private static Flight createTestFlight(String flightNumber,
                                           String departureTime,
                                           String arrivalTime,
                                           int i) {
        // Creating a sample Flight record
        return Flight.newBuilder()
                .setFlightNumber(flightNumber)
                .setDepartureTime(departureTime)
                .setArrivalTime(arrivalTime)
                .setAirline("TestAirline")
                .setDepartureAirport("JFK")
                .setArrivalAirport("LAX")
                .setFlightCategory(FlightCategory.PASSENGER)
                .build();
    }
}
