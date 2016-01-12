package io.stat.nabuproject.nabu.kafka;

import io.stat.nabuproject.nabu.common.NabuCommand;
import io.stat.nabuproject.nabu.protocol.CommandEncoder;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * A Kafka serializer for NabuCommands.
 * For any writes that Nabu throws into a throttle queue, we just
 * throw the raw NabuCommand into Kafka to save a bit on the performance cost
 * of storing the command in some intermedia representation. Also, this class is effectively
 * a proxy for a NabuCommand serializer.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class NabuCommandKafkaSerializer implements Serializer<NabuCommand> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        /* no-op */
    }

    @Override
    public byte[] serialize(String topic, NabuCommand data) {
        try {
            if (data == null)
                return null;
            else
                return new CommandEncoder().performEncode(data);
        } catch (Exception e) {
            throw new SerializationException("Error when serializing NabuCommand to byte[]", e);
        }
    }

    @Override
    public void close() {
        /* no-op */
    }
}
