package io.stat.nabuproject.enki.leader;

import java.util.List;

/**
 * Something which can respond to Enki leader-election related events
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface LeaderEventListener {
    /**
     * Called when the leader has changed.
     * @param isSelf whether or not this node is the leader.
     * @param myData the LeaderData of the new leader
     * @param allLeaderData the leaderDatas of all the nodes.
     * @return true if the callback was successful, false otherwise.
     */
    boolean onLeaderChange(boolean isSelf, LeaderData myData, List<LeaderData> allLeaderData);
}
