package io.stat.nabuproject.core.net;

import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;

/**
 * A basic {@link AdvertisedAddressProvider} which sources its information
 * directly from a {@link NetworkServerConfigProvider}
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@RequiredArgsConstructor(onConstructor=@__(@Inject))
class SimpleAAPImpl implements AdvertisedAddressProvider {
    private final NetworkServerConfigProvider nscp;

    @Override
    public AddressPort getAdvertisedAddress() {
        return nscp.getListenBinding();
    }
}
