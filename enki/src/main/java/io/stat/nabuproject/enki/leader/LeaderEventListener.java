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

    /**
     * Called when this node has been demoted from the leader, whether due to connection loss or
     * other form of preemption. Listeners to this should immediately disconnect their clients and either
     * redirect them to themselves so they can then REDIRECT to the new leader... or, just stop serving connections
     * alltogether Likewise, integrators should invalidate their caches and wait for the next onLeaderChange before
     * serving requests/activating.
     * @param lastKnownData the last known data for this node.
     * @return true if the callback was successful, false otherwise. As a failsafe, any failures
     *              or exceptions thrown from this callback will cause Enki to shutdown, to reduce the
     *              risk of a split-brain situation.
     */
    boolean onSelfDemoted(LeaderData lastKnownData);
}
