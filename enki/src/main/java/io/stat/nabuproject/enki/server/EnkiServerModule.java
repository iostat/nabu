package io.stat.nabuproject.enki.server;

import com.google.inject.AbstractModule;

/**
 * Created by io on 12/28/15. io is an asshole because
 * he doesn't write documentation for his code.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class EnkiServerModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(EnkiServer.class).to(ServerImpl.class).asEagerSingleton();
    }
}