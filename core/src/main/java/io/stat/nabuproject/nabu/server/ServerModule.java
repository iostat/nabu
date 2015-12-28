package io.stat.nabuproject.nabu.server;

import com.google.inject.AbstractModule;

/**
 * Created by io on 12/26/15. io is an asshole for
 * not giving writing documentation for his code.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class ServerModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(NabuServer.class).to(ServerImpl.class).asEagerSingleton();
    }
}
