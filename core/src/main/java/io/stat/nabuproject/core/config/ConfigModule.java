package io.stat.nabuproject.core.config;

import com.google.inject.AbstractModule;
import io.stat.nabuproject.core.elasticsearch.ESConfigProvider;

/**
 * Guice module for the Config subsystem.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class ConfigModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(ConfigStore.class).to(YamlConfigStore.class);

        bind(ESConfigProvider.class).to(AbstractConfig.class);
    }
}
