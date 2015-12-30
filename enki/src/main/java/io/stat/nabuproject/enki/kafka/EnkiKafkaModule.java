package io.stat.nabuproject.enki.kafka;

import com.google.inject.AbstractModule;

/**
 * Created by io on 12/30/15. io is an asshole because
 * he doesn't write documentation for his code.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class EnkiKafkaModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(KafkaMetadataClient.class).asEagerSingleton();
    }
}
