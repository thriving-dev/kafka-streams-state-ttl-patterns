package dev.thriving.poc;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.Optional;

public class LazySerde<T> implements Serde<LazySerde.Lazy<T>> {

    private final Serde<T> innerSerde;

    public LazySerde(Serde<T> inner) {
        innerSerde = inner;
    }

    @Override
    public Serializer<Lazy<T>> serializer() {
        return (topic, data) -> innerSerde.serializer().serialize(topic, data.get());
    }

    @Override
    public Deserializer<Lazy<T>> deserializer() {
        return (topic, data) ->
                new Lazy<>(data, (t, d) -> innerSerde.deserializer().deserialize(t, d));
    }

    public static class Lazy<T> {
        private final byte[] data;
        private final Deserializer<T> deserializer;
        private Optional<T> cachedValue = Optional.empty();

        public Lazy(byte[] data, Deserializer<T> deserializer) {
            this.data = data;
            this.deserializer = deserializer;
        }

        public Lazy(T value) {
            this.cachedValue = Optional.of(value);
            data = null;
            deserializer = null;
        }

        public T get() {
            if (cachedValue.isEmpty()) {
                cachedValue = Optional.ofNullable(deserializer.deserialize(null, data));
            }
            return cachedValue.orElse(null);
        }

        public boolean isPresent() { return cachedValue.isPresent(); }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        innerSerde.configure(configs, isKey);
    }

    @Override
    public void close() {
        innerSerde.close();
    }
}
