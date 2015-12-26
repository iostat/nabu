package io.stat.nabu.kafka;

import com.google.inject.AbstractModule;

/**
 * Created by io on 12/26/15. io is an asshole for
 * not giving writing documentation for his code.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class KafkaModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(NabuConsumerCoordinator.class).to(ConsumerCoordinatorImpl.class).asEagerSingleton();
        bind(NabuKafkaProducer.class).to(KafkaProducerImpl.class).asEagerSingleton();
    }
}
