package io.stat.nabuproject.core.throttling;

/**
 * Something which can be notified of changes to ThrottlePolicies
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface ThrottlePolicyChangeListener {
    /**
     * Called when a throttle policy has changed. Because holding onto atomic references
     * to the policies is the preferred method of, no data is actually provided.
     * (todo: this is probably stupid)
     * @return true if the actions this listener takes have succeeded, false otherwise.
     */
    boolean onThrottlePolicyChange();
}
