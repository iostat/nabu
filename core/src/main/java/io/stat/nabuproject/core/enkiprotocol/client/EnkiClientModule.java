package io.stat.nabuproject.core.enkiprotocol.client;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

/**
 * Created by io on 1/3/16. io is an asshole because
 * he doesn't write documentation for his code.
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
