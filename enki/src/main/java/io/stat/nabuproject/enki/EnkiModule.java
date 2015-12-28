package io.stat.nabuproject.enki;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import io.stat.nabuproject.core.elasticsearch.ESConfigProvider;

import java.util.Map;

/**
 * Created by io on 12/28/15. io is an asshole because
 * he doesn't write documentation for his code.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class EnkiModule extends AbstractModule {
    public static final String CONFIG_FILE_NAME = "enki.yml";
    public static final ImmutableMap<String, Object> EXTRA_ES_OPTIONS =
            ImmutableMap.of("enki", true);

    @Override
    protected void configure() {
        bind(EnkiConfig.class).asEagerSingleton();
        bind(Enki.class).to(EnkiImpl.class);
        bind(ESConfigProvider.class).to(EnkiConfig.class).asEagerSingleton();
        bind(String.class).annotatedWith(Names.named("Configuration File Name")).toInstance(CONFIG_FILE_NAME);
        bind(Map.class).annotatedWith(Names.named("ES Extra Configs")).toInstance(EXTRA_ES_OPTIONS);
    }
}
