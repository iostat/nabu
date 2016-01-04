package io.stat.nabuproject.enki.leader;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

/**
 * A Guice module for the leader election subsystem.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class LeaderElectionModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(ZKLeaderImpl.class).in(Singleton.class);
        bind(ElectedLeaderProvider.class).to(ZKLeaderImpl.class);
        bind(EnkiLeaderElector.class).to(ZKLeaderImpl.class);
    }
}
