package io.stat.nabu.config;

import com.google.inject.AbstractModule;

/**
 * Guice module for the Config subsystem.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class ConfigModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(ConfigurationProvider.class).to(YamlConfigProvider.class).asEagerSingleton();
    }
}
