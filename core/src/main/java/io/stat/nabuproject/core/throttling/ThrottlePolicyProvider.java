package io.stat.nabuproject.core.throttling;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Something which can provide @{link ThrottlePolicy}s.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface ThrottlePolicyProvider {
    /**
     * Get a list of AtomicReferences to throttle policies this provider uses, so
     * that they can be refreshed without needing to re-query.
     */
    List<AtomicReference<ThrottlePolicy>> getTPReferences();

    /**
     * Gets a list of all throttle policies this provider is aware of. Note that by
     * default this performs an immutable list clone of getTPReferences.
     */
    default List<ThrottlePolicy> getThrottlePolicies() {
        // todo: make immutablemap collector.
        return ImmutableList.copyOf(getTPReferences().stream().map(AtomicReference::get).collect(Collectors.toList()));
    }

    /**
     * Checks if this provider is aware of a throttle policy for the specified index
     * @param indexName the index to look up
     * @return whether or not the specified index has a throttle policy associated with it.
     */
    default boolean doesIndexHaveTP(String indexName) {
        return getTPReferences()
                .stream().anyMatch(tp -> tp.get().getIndexName().equals(indexName));
    }

    /**
     * Attempts to find a throttle policy for the specified ES index.
     * @param indexName the index to attempt to find the throttle policy for
     * @return a ThrottlePolicy if it exists for indexName, or else null.
     */
    default AtomicReference<ThrottlePolicy> getTPForIndex(String indexName) {
        return getTPReferences()
                .stream().filter(tp -> tp.get().getIndexName().equals(indexName)).findFirst().orElse(null);
    }

    /**
     * Register a TPCL
     * @param tpcl the TPCL to register.
     */
    default void registerThrottlePolicyChangeListener(ThrottlePolicyChangeListener tpcl) {
        LoggerFactory.getLogger(ThrottlePolicyProvider.class).warn("{} called the default implementation of deregisterTPCL(), which is probably not what you want.", this.getClass());
    }

    /**
     * Deregister a TPCL.
     * @param tpcl the TPCL to stop sending change events to.
     */
    default void deregisterThrottlePolicyChangeListener(ThrottlePolicyChangeListener tpcl) {
        LoggerFactory.getLogger(ThrottlePolicyProvider.class).warn("{} called the default implementation of deregisterTPCL(), which is probably not what you want.", this.getClass());
    }

    /**
     * Gets all TPCLs registered to this TPP. This is meant to be used internally to allow a DynamicTPP to reroute listeners when the
     * provider is changed dynamically.
     */
    default Collection<ThrottlePolicyChangeListener> getAllTPCLs() {
        LoggerFactory.getLogger(ThrottlePolicyProvider.class).warn("{} called the default implementation of getAllTPCLs(), which is probably not what you want.", this.getClass());
        return ImmutableList.of();
    }

    /**
     * Like with {@link ThrottlePolicyProvider#getAllTPCLs()}, this is really meant for internal use, and should replace the list of all
     * TPCLs registered.
     */
    default void seedTPCLs(Collection<ThrottlePolicyChangeListener> tpcls) {
        LoggerFactory.getLogger(ThrottlePolicyProvider.class).warn("{} called the default implementation of seedTPCLs(), which is probably not what you want.", this.getClass());
    }

    /**
     * Performs a merge of a list of received throttle policies into a list of atomicreferences to throttle policies.
     * This does not do any sanity checks other than a stern error message to the console.
     * @param newTPs The new TPs received from where
     * @param sourcedThrottlePolicies a list of existing TPs that we want to update
     * @param shouldUpsert whether or not to create a policy if it doesn't exist.
     * @param logger a logger to dump a stern warning into.
     */
    static void performTPMerge(List<ThrottlePolicy> newTPs, Collection<AtomicReference<ThrottlePolicy>> sourcedThrottlePolicies, boolean shouldUpsert, Logger logger) {
        AtomicInteger updatedTPs = new AtomicInteger(0);
        List<AtomicReference<ThrottlePolicy>> upserts = Lists.newArrayList();
        newTPs.forEach(newtp -> {
            AtomicBoolean wasUpdated = new AtomicBoolean(false);
            sourcedThrottlePolicies.forEach(ref -> {
                if(wasUpdated.get()) { return; }
                if(ref.get().getIndexName().equals(newtp.getIndexName())) {
                    ref.set(newtp);
                    logger.info("Updated TP {}", newtp);
                    wasUpdated.set(true);
                    updatedTPs.incrementAndGet();
                }
            });

            if(!wasUpdated.get()) {
                logger.info("Upserted TP {}", newtp);
                upserts.add(new AtomicReference<>(newtp));
                if(shouldUpsert) {
                    updatedTPs.incrementAndGet();
                }
            }
        });

        if(shouldUpsert) {
            sourcedThrottlePolicies.addAll(upserts);
        } else {
            if(upserts.size() != 0) {
                logger.warn("Skipping {} upserts. This may not be what you want!");
            }
        }

        if(!(newTPs.size() == sourcedThrottlePolicies.size() && sourcedThrottlePolicies.size() == updatedTPs.get())) {
            logger.error("Not all throttle policies were updated. This is beyond impossible and sensible");
        }
    }
}
