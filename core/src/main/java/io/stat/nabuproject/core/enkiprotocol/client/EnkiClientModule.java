package io.stat.nabuproject.core.enkiprotocol.client;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import io.stat.nabuproject.core.enkiprotocol.dispatch.EnkiClientEventSource;

/**
 * A Guice module for the Enki protocol client.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class EnkiClientModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(ClientImpl.class).in(Singleton.class);
        bind(EnkiClient.class).to(ClientImpl.class);
        bind(EnkiClientEventSource.class).to(ClientImpl.class);
    }
}
