package io.stat.nabuproject.core.kafka;

import kafka.utils.ZKStringSerializer;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

/**
 * A ZkSerializer implementation that proxies calls to
 * {@link kafka.utils.ZKStringSerializer}.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/) (although, lets be honest, the Kafka team did all the hard work)
 */
public final class KafkaZKStringSerializerProxy implements ZkSerializer {
    @Override
    public byte[] serialize(Object o) throws ZkMarshallingError {
        return ZKStringSerializer.serialize(o);
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        return ZKStringSerializer.deserialize(bytes);
    }
}
