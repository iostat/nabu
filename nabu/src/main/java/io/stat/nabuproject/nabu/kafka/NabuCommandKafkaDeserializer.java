package io.stat.nabuproject.nabu.kafka;

import io.stat.nabuproject.nabu.common.NabuCommand;
import io.stat.nabuproject.nabu.protocol.CommandDecoder;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Created by io on 1/11/16. io is an asshole because
 * he doesn't write documentation for his code.
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
            throw new SerializationException("Error when deserializing byte[] to NabuCommand for Kafka", e);
        }
    }

    @Override
    public void close() {
        /* no-op */
    }
}
