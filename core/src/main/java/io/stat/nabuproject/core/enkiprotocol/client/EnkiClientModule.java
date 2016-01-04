package io.stat.nabuproject.core.enkiprotocol.client;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

/**
 * A Guice module for the Enki protocol client.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class EnkiClientModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(EnkiClientImpl.class).in(Singleton.class);
        bind(EnkiClient.class).to(EnkiClientImpl.class);
    }
}
