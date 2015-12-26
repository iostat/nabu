package io.stat.nabu.elasticsearch;

import com.google.inject.AbstractModule;

/**
 * Guice module for Elasticsearch subsytem
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class ESModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(ESClient.class).to(NodeClientImpl.class).asEagerSingleton();
    }
}
