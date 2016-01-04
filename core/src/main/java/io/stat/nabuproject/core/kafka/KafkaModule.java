package io.stat.nabuproject.core.kafka;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

/**
 * Guice module for the Kafka module.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class KafkaModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(KafkaMetadataClientImpl.class).in(Singleton.class);
        bind(KafkaMetadataClient.class).to(KafkaMetadataClientImpl.class);
    }
}
