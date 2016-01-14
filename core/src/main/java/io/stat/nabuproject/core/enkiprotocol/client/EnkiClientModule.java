package io.stat.nabuproject.core.enkiprotocol.client;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import io.stat.nabuproject.core.enkiprotocol.dispatch.EnkiClientEventSource;
import io.stat.nabuproject.core.kafka.KafkaBrokerConfigProvider;
import io.stat.nabuproject.core.throttling.ThrottlePolicyProvider;
import lombok.RequiredArgsConstructor;

/**
 * A Guice module for the Enki protocol client.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@RequiredArgsConstructor
public class EnkiClientModule extends AbstractModule {
    private final boolean bindAsKafkaBrokerProvider;
    private final boolean bindAsThrottlePolicyProvider;

    @Override
    protected void configure() {
        bind(ClientImpl.class).in(Singleton.class);
        bind(EnkiClient.class).to(ClientImpl.class);
        bind(EnkiClientEventSource.class).to(ClientImpl.class);

        if(bindAsKafkaBrokerProvider) {
            bind(KafkaBrokerConfigProvider.class).to(ClientImpl.class);
        }

        if(bindAsThrottlePolicyProvider) {
            bind(ThrottlePolicyProvider.class).to(ClientImpl.class);
        }
    }
}
