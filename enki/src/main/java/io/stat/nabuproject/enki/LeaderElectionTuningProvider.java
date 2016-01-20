package io.stat.nabuproject.enki;

/**
 * Provides tunable properties for the leader election system.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface LeaderElectionTuningProvider {
    /**
     * Amount of time to sleep before and after each read to allow write operations to happen
     * before data is returned.
     * The idea being, this reduces the risk of a race condition leading to stale data wherein nothing has updated
     * the longegrated leader state prior to a read lock being obtained, or it was updated shortly after the read
     * finished.
     */
    long getSettleDelay();

    /**
     * How long to wait before retrying ZK reconciliation queue
     * entries if there's no match yet.
     */
    long getZKReconcileDelay();

    /**
     * How to long to wait before retrying finding
     * a leader while one is not available.
     */
    long getNoLeaderRetryDelay();

    /**
     * Maximum amount of times to retry getting a leader before giving up
     */
    long getMaxLeaderRetries();

    /**
     * How long to hold any leader requests after a node departs from ES to allow ZK to try and catch up.
     */
    long getESEventSyncDelay();

    /**
     * How long between when ES_EVENT_SYNC_DELAYs can be re-triggered. Used in order
     * to prevent a flapping Enki from DoSing provisioning of leaders to Nabus
     */
    long getESEventSyncDelayGap();

    /**
     * How long to allow a cache-buster task to run when ZK says we've been demoted
     * before considering it too big a risk of split-brain and killing the app.
     */
    long getMaxDemotionFailsafeTimeout();
}
