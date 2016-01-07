package io.stat.nabuproject.core.net;

import com.google.inject.AbstractModule;
import lombok.RequiredArgsConstructor;

/**
 * A Guice module which allows the "default" {@link AdvertisedAddressProvider} to be
 * injected, which sources its information from a {@link NetworkServerConfigProvider}
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@RequiredArgsConstructor
public class AdvertisedAddressResolverModule extends AbstractModule {
    public enum Resolver {
        PASSTHROUGH,
        MOST_ACCESSIBLE;
    }

    private final Resolver requestedResolver;

    @Override
    protected void configure() {
        Class<? extends AdvertisedAddressProvider> binding;

        switch(requestedResolver) {
            case MOST_ACCESSIBLE:
                binding = MostAccessibleAddressResolver.class;
                break;
            case PASSTHROUGH:
            default:
                binding = SimpleAAPImpl.class;
                break;
        }

        bind(AdvertisedAddressProvider.class).to(binding);
    }
}
