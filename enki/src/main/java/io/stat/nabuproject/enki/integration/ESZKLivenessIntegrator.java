package io.stat.nabuproject.enki.integration;

import io.stat.nabuproject.core.net.AddressPort;
import io.stat.nabuproject.enki.leader.ElectedLeaderProvider;

/**
 *
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class ESZKLivenessIntegrator implements ElectedLeaderProvider {
    @Override
    public boolean isSelf() {
        return false;
    }

    @Override
    public AddressPort getElectedLeaderAP() {
        return null;
    }
}
