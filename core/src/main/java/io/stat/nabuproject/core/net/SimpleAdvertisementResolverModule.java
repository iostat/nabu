package io.stat.nabuproject.core.net;

import com.google.inject.AbstractModule;

/**
 * A Guice module which allows the "default" {@link AdvertisedAddressProvider} to be
 * injected, which sources its information from a {@link NetworkServerConfigProvider}
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class SimpleAdvertisementResolverModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(AdvertisedAddressProvider.class).to(SimpleAAPImpl.class);
    }
}
