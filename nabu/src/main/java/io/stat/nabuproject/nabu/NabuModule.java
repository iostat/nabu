package io.stat.nabuproject.nabu;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import io.stat.nabuproject.core.config.AbstractConfig;

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
        bind(NabuConfig.class).in(Singleton.class);
        bind(AbstractConfig.class).to(NabuConfig.class);

        bind(String.class).annotatedWith(Names.named("Configuration File Name")).toInstance(CONFIG_FILE_NAME);
        bind(new TypeLiteral<Map<String, Object>>() {}).annotatedWith(Names.named("ES Extra Configs")).toInstance(EXTRA_ES_OPTIONS);

        bind(Nabu.class).to(NabuImpl.class).asEagerSingleton();
    }
}
