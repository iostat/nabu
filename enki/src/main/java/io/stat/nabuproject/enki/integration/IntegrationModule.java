package io.stat.nabuproject.enki.integration;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import io.stat.nabuproject.core.Component;
import io.stat.nabuproject.enki.leader.ElectedLeaderProvider;

/**
 * Allows for injecting of all the subsytem integrators.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class IntegrationModule extends AbstractModule {
    private final boolean useESZKLivenessIntegrator;

    public IntegrationModule(boolean useESZKLivenessIntegrator) {
        this.useESZKLivenessIntegrator = useESZKLivenessIntegrator;
    }

    @Override
    protected void configure() {
        bind(ESZKLeaderIntegrator.class).in(Singleton.class);
        bind(ESKafkaValidatorImpl.class).in(Singleton.class);

        bind(ElectedLeaderProvider.class).to(LeaderLivenessIntegrator.class);

        bind(ZKThrottlePolicyProvider.class).in(Singleton.class);
        bind(Component.class).annotatedWith(Names.named("ZKTPP")).to(ZKThrottlePolicyProvider.class);

        bind(ESKafkaValidator.class).to(ESKafkaValidatorImpl.class);
        if(useESZKLivenessIntegrator) {
            bind(LeaderLivenessIntegrator.class).to(ESZKLeaderIntegrator.class);
        }
    }
}
