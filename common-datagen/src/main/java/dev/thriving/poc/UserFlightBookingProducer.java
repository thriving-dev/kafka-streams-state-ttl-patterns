package dev.thriving.poc;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedEpochGenerator;
import dev.thriving.poc.avro.ContactPreference;
import dev.thriving.poc.avro.FlightClass;
import dev.thriving.poc.avro.UserFlightBooking;
import dev.thriving.poc.utils.AdjustableUUIDClock;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class UserFlightBookingProducer {

    public static void main(String[] args) {
        String topicName = "user-flight-booking";
        String schemaRegistryUrl = "http://localhost:8081";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        // Initialize the producer
        Producer<String, UserFlightBooking> producer = new KafkaProducer<>(props);

        // Clock for generating UUID v7
        AdjustableUUIDClock uuidClock = new AdjustableUUIDClock();
        uuidClock.shiftTime(Duration.ofMinutes(-1));
        TimeBasedEpochGenerator uuid7Generator = Generators.timeBasedEpochGenerator(null, uuidClock);

        LocalDate startDate = LocalDate.now().minusDays(14); // Start 14 days in the past

        // Produce 10 records
        for (int i = 0; i < 30; i++) {
            // Create a BaggageTracking record
            UserFlightBooking record = createTestUserFlightBooking(uuid7Generator, startDate, i);

            // shift time
            uuidClock.shiftTime(Duration.ofSeconds(6));

            // Create a producer record with the key and value
            ProducerRecord<String, UserFlightBooking> producerRecord = new ProducerRecord<>(
                    topicName,
                    null,
                    uuidClock.currentTimeMillis(),
                    record.getBookingId(),
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

    private static UserFlightBooking createTestUserFlightBooking(TimeBasedEpochGenerator uuid7Generator,
                                                                 LocalDate startDate,
                                                                 int i) {
        // Creating a sample UserFlightBooking record
        return UserFlightBooking.newBuilder()
                .setBookingId(uuid7Generator.generate().toString())
                .setUserId(UUID.randomUUID().toString())
                .setFlightNumber("FL" + (1000 + new Random().nextInt(9000)))
                .setDepartureDate(startDate.plusDays(i).format(DateTimeFormatter.ISO_LOCAL_DATE))
                .setSeatNumber("12A")
                .setReservationCode("ABC" + i)
                .setFlightClass(FlightClass.ECONOMY)
                .setEmail("test@example.com")
                .setPhoneNumber("+123456789")
                .setDeviceID("device-1234")
                .setPreferredLanguage("en")
                .setContactPreference(ContactPreference.EMAIL)
                .build();
    }
}
