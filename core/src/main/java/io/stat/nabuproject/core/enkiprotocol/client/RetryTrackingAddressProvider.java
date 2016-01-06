package io.stat.nabuproject.core.enkiprotocol.client;

import io.stat.nabuproject.core.enkiprotocol.EnkiAddressProvider;
import io.stat.nabuproject.core.net.AddressPort;
import lombok.Getter;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.stat.nabuproject.core.util.functional.FluentCompositions.not;

/**
 * Takes an EnkiAddressProvider, and gets the first eligible Enki
 * node that we have successfully connected to since the last connection was made.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
final class RetryTrackingAddressProvider implements EnkiAddressProvider {
    private final @Getter
    Set<AddressPort> blacklistedAPs;
    private final EnkiAddressProvider provider;

    RetryTrackingAddressProvider(EnkiAddressProvider provider) {
        this.provider = provider;
        this.blacklistedAPs = new HashSet<>();
    }

    void connectionSuccessful() {
        blacklistedAPs.clear();
    }

    @Override
    public boolean isEnkiDiscovered() {
        return provider.isEnkiDiscovered() && provider.getDiscoveredEnkis().size() != blacklistedAPs.size();
    }

    @Override
    public List<AddressPort> getDiscoveredEnkis() {
        return provider.getDiscoveredEnkis();
    }

    public AddressPort getNextEnki() {
        AddressPort candidate = provider.getDiscoveredEnkis()
                                        .stream()
                                        .filter(not(blacklistedAPs::contains))
                                        .findFirst()
                                        .orElse(null);
        if(candidate != null) {
            blacklistedAPs.add(candidate);
        }

        return candidate;
    }
}
