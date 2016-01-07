package io.stat.nabuproject.enki.leader;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

/**
 * A Guice module for the leader election subsystem.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class ZKLeaderElectionModule extends AbstractModule {
    private final boolean useZKAsElectedLeaderProvider;

    /**
     * calls {@link this#ZKLeaderElectionModule(boolean)} with <tt>false</tt>
     */
    public ZKLeaderElectionModule() {
        this(false);
    }

    /**
     * Create a new ZKLeaderElectionModule
     * @param ZKImplAsElector whether or not to bind the ZooKeeper leader elector as the injected {@link ElectedLeaderProvider}
     *                        Set this to <tt>false</tt> if you will use some other integration for leader provision, such as
     *                        the ESZKLivenessIntegrator (the default)
     */
    public ZKLeaderElectionModule(boolean ZKImplAsElector) {
        this.useZKAsElectedLeaderProvider = ZKImplAsElector;
    }

    @Override
    protected void configure() {
        bind(ZKLeaderImpl.class).in(Singleton.class);
        bind(ZKLeaderProvider.class).to(ZKLeaderImpl.class);
        bind(EnkiLeaderElector.class).to(ZKLeaderImpl.class);

        if(useZKAsElectedLeaderProvider) {
            bind(ElectedLeaderProvider.class).to(ZKLeaderImpl.class);
        }
    }
}
