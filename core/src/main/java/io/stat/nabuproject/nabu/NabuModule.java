package io.stat.nabuproject.nabu;

import com.google.inject.AbstractModule;

/**
 * Guice module for the Nabu core.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class NabuModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(Nabu.class).to(NabuImpl.class).asEagerSingleton();
    }
}