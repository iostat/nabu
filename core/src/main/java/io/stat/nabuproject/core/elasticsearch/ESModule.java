package io.stat.nabuproject.core.elasticsearch;

import com.google.inject.AbstractModule;
import io.stat.nabuproject.core.enkiprotocol.EnkiAddressProvider;

/**
 * Guice module for Elasticsearch subsytem
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class ESModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(NodeClientImpl.class).asEagerSingleton();
        bind(ESClient.class).to(NodeClientImpl.class);
        bind(EnkiAddressProvider.class).to(NodeClientImpl.class);
    }
}
