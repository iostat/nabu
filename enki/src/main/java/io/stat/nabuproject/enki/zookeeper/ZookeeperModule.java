package io.stat.nabuproject.enki.zookeeper;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

/**
 * Guice bindings for the Zookeeper component abstraction
 * and ZK-sourced throttle policy providers
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class ZookeeperModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(ZKClientImpl.class).in(Singleton.class);
        bind(ZKClient.class).to(ZKClientImpl.class);
    }
}
