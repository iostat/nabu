package io.stat.nabuproject.enki.leader;

/**
 * Something which can provide information on who the leader is amongst a group of Enkis.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface ElectedLeaderProvider {
    boolean isSelf();
    String getLeaderAddress();
    int getLeaderPort();
}
