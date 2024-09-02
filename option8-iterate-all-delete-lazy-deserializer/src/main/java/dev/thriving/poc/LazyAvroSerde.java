package dev.thriving.poc;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.Optional;

public class LazyAvroSerde<T extends SpecificRecord> implements Serde<LazyAvroSerde.Lazy<T>> {

    private final SpecificAvroSerde<T> innerSerde;

    public LazyAvroSerde(SchemaRegistryClient client, Map<String, ?> serdeConfig) {
        innerSerde = new SpecificAvroSerde<>(client);
        innerSerde.configure(serdeConfig, false);
    }

    @Override
    public Serializer<Lazy<T>> serializer() {
        return (topic, data) -> innerSerde.serializer().serialize(topic, data.get());
    }

    @Override
    public Deserializer<Lazy<T>> deserializer() {
        return (topic, data) -> new Lazy<>(data, (t, d) -> innerSerde.deserializer().deserialize(t, d));
    }

    public static class Lazy<T> {
        private final byte[] data;
        private final Deserializer<T> deserializer;
        private Optional<T> cachedValue = Optional.empty();

        public Lazy(byte[] data, Deserializer<T> deserializer) {
            this.data = data;
            this.deserializer = deserializer;
        }

        public T get() {
            if (!cachedValue.isPresent()) {
                cachedValue = Optional.ofNullable(deserializer.deserialize(null, data));
            }
            return cachedValue.orElse(null);
        }

        public boolean isPresent() {
            return cachedValue.isPresent();
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        innerSerde.configure(configs, isKey);
    }

    @Override
    public void close() {
        innerSerde.close();
    }

//    @Override
//    public Serializer<Lazy<T>> serializer() {
//        return this.serializer();
//    }
//
//    @Override
//    public Deserializer<Lazy<T>> deserializer() {
//        return this.deserializer();
//    }
}
