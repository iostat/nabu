package io.stat.nabuproject.core.net;

import com.google.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;

/**
 * An {@link AdvertisedAddressProvider} which attempts to resolve
 * an address like 0.0.0.0 to the most accessible address.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
class MostAccessibleAddressResolver implements AdvertisedAddressProvider {
    private final AddressPort resolvedBinding;

    @Inject
    MostAccessibleAddressResolver(NetworkServerConfigProvider nscp) {
        AddressPort ap = nscp.getListenBinding();
        int port = ap.getPort();
        String binding = ap.getAddress();
        if (binding.equals("0.0.0.0") ||
                binding.equals("::")  ||
                binding.equals("0:0:0:0:0:0:0:0")) {
            try {
                String newBinding = InetAddress.getLocalHost().getHostAddress();
                logger.info("Resolved {} to {}", binding, newBinding);
                binding = newBinding;
            } catch(Exception e) {
                logger.error("Received an Exception when attempting to resolve an unspecified IP address", e);
            } finally {
                resolvedBinding = new AddressPort(binding, port);
            }
        } else {
            logger.info("Bound IP ({}) is not an unspecified IP, not resolving.", binding);
            resolvedBinding = new AddressPort(binding, port);
        }
    }

    @Override
    public AddressPort getAdvertisedAddress() {
        return resolvedBinding;
    }
}
