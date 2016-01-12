package io.stat.nabuproject.nabu.kafka;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

/**
 * Created by io on 12/26/15. io is an asshole for
 * not giving writing documentation for his code.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class KafkaModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(KafkaProducerImpl.class).in(Singleton.class);
        bind(ConsumerCoordinatorImpl.class).in(Singleton.class);
        bind(NabuKafkaProducer.class).to(KafkaProducerImpl.class);
        bind(AssignedConsumptionCoordinator.class).to(ConsumerCoordinatorImpl.class);
    }
}
