package io.stat.nabuproject.nabu;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import io.stat.nabuproject.core.config.AbstractConfig;
import io.stat.nabuproject.core.net.NetworkServerConfigProvider;


/**
 * Guice module for the Nabu core.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class NabuModule extends AbstractModule {
    public static final String CONFIG_FILE_NAME = "nabu.yml";

    @Override
    protected void configure() {
        bind(NabuImpl.class).in(Singleton.class);
        bind(Nabu.class).to(NabuImpl.class);

        bind(NabuConfig.class).in(Singleton.class);
        bind(AbstractConfig.class).to(NabuConfig.class);
        bind(NetworkServerConfigProvider.class).to(NabuConfig.class);

        bind(String.class).annotatedWith(Names.named("Configuration File Name")).toInstance(CONFIG_FILE_NAME);
    }
}
