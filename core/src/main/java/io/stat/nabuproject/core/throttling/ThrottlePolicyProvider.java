package io.stat.nabuproject.core.throttling;

import java.util.List;

/**
 * Something which can provide @{link ThrottlePolicy}s.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface ThrottlePolicyProvider {
    /**
     * Gets a list of all throttle policies this provider is aware of.
     */
    List<ThrottlePolicy> getThrottlePolicies();

    /**
     * Checks if this provider is aware of a throttle policy for the specified index
     * @param indexName the index to look up
     * @return whether or not the specified index has a throttle policy associated with it.
     */
    default boolean doesIndexHaveTP(String indexName) {
        return getThrottlePolicies()
                .stream().anyMatch(tp -> tp.getIndexName().equals(indexName));
    }

    /**
     * Attempts to find a throttle policy for the specified ES index.
     * @param indexName the index to attempt to find the throttle policy for
     * @return a ThrottlePolicy if it exists for indexName, or else null.
     */
    default ThrottlePolicy getTPForIndex(String indexName) {
        return getThrottlePolicies()
                .stream().filter(tp -> tp.getIndexName().equals(indexName)).findFirst().orElse(null);
    }
}
