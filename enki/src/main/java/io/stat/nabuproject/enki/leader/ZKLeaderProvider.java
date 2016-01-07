package io.stat.nabuproject.enki.leader;

/**
 * An interface that describes something which can perform Enki leader related
 * operations on ZooKeeper
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface ZKLeaderProvider extends ElectedLeaderProvider, LeaderEventSource {

}
