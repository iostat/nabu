package io.stat.nabuproject.enki;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import io.stat.nabuproject.core.config.AbstractConfig;
import io.stat.nabuproject.core.kafka.KafkaZkConfigProvider;
import io.stat.nabuproject.core.net.NetworkServerConfigProvider;
import io.stat.nabuproject.enki.leader.ZKLeaderConfigProvider;

/**
 * Guice module for the Enki bootstrap.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class EnkiModule extends AbstractModule {
    public static final String CONFIG_FILE_NAME = "enki.yml";

    @Override
    protected void configure() {
        bind(EnkiImpl.class).in(Singleton.class);
        bind(Enki.class).to(EnkiImpl.class);

        bind(EnkiConfig.class).in(Singleton.class);
        bind(AbstractConfig.class).to(EnkiConfig.class);
        bind(KafkaZkConfigProvider.class).to(EnkiConfig.class);
        bind(ZKLeaderConfigProvider.class).to(EnkiConfig.class);
        bind(NetworkServerConfigProvider.class).to(EnkiConfig.class);

        bind(String.class).annotatedWith(Names.named("Configuration File Name")).toInstance(CONFIG_FILE_NAME);
    }
}
