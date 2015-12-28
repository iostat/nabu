package io.stat.nabuproject.nabu;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import io.stat.nabuproject.core.elasticsearch.ESConfigProvider;

import java.util.Map;


/**
 * Guice module for the Nabu core.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class NabuModule extends AbstractModule {
    public static final String CONFIG_FILE_NAME = "nabu.yml";
    public static final ImmutableMap<String, Object> EXTRA_ES_OPTIONS =
            ImmutableMap.of("nabu", true);

    @Override
    protected void configure() {
        bind(NabuConfig.class).asEagerSingleton();
        bind(Nabu.class).to(NabuImpl.class).asEagerSingleton();
        bind(ESConfigProvider.class).to(NabuConfig.class).asEagerSingleton();
        bind(String.class).annotatedWith(Names.named("Configuration File Name")).toInstance(CONFIG_FILE_NAME);
        bind(Map.class).annotatedWith(Names.named("ES Extra Configs")).toInstance(EXTRA_ES_OPTIONS);
    }
}