package io.stat.nabuproject.enki.leader;

import io.stat.nabuproject.core.net.AddressPort;

/**
 * Something which can provide information on who the leader is amongst a group of Enkis.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface ElectedLeaderProvider {
    boolean isSelf();
    AddressPort getElectedLeaderAP();
}
