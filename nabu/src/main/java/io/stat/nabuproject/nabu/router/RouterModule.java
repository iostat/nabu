package io.stat.nabuproject.nabu.router;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

/**
 * The Guice module for the {@link io.stat.nabuproject.nabu.common.command.NabuCommand}
 * router which takes incoming packets from the Nabu server and figures out what to do with them
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class RouterModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(RouterImpl.class).in(Singleton.class);
        bind(CommandRouter.class).to(RouterImpl.class);
    }
}
