package io.stat.nabuproject.nabu.kafka;

import io.stat.nabuproject.nabu.common.command.NabuCommand;
import io.stat.nabuproject.nabu.protocol.CommandDecoder;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * A Kafka deserializer for {@link NabuCommand}s
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class NabuCommandKafkaDeserializer implements Deserializer<NabuCommand> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        /* no-op */
    }

    @Override
    public NabuCommand deserialize(String topic, byte[] data) {
        try {
            if (data == null)
                return null;
            else
                return new CommandDecoder().performDecode(data);
        } catch (Exception e) {
            throw new SerializationException("Error when deserializing byte[] to NabuWriteCommand for Kafka", e);
        }
    }

    @Override
    public void close() {
        /* no-op */
    }
}
