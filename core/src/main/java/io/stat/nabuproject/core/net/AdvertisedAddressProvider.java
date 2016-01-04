package io.stat.nabuproject.core.net;

/**
 * Something which can provide an address to advertise, for instance, in say,
 * the leader advertisements. This can be as simple as whatever is configured in the
 * config file, or something fancy which looks at all the IPs on the system and determines
 * which is the most reachable, etc.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface AdvertisedAddressProvider {
    AddressPort getAdvertisedAddress();
}
