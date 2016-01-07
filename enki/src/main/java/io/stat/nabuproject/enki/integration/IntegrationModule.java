package io.stat.nabuproject.enki.integration;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
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
        bind(ElectedLeaderProvider.class).to(LeaderLivenessIntegrator.class);
        if(useESZKLivenessIntegrator) {
            bind(LeaderLivenessIntegrator.class).to(ESZKLeaderIntegrator.class);
        }
    }
}
