package io.stat.nabuproject.enki.integration;

/**
 * Provides configuration specific to the {@link WorkerCoordinator}
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface WorkerCoordinatorConfigProvider {
    /**
     * How often to cycle the rebalancer, to allow worker joins
     * to coordinate.
     */
    long getRebalancePeriod();

    /**
     * How long to wait before killing the rebalancer when shutting down.
     * This needs to be tunable as the larger the task:worker ratio,
     * the time to ACK every UNASSIGN grows.
     */
    long getRebalanceKillTimeout();
}
