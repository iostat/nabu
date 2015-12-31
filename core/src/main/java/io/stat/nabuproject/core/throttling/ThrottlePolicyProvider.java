package io.stat.nabuproject.core.throttling;

import java.util.List;

/**
 * Something which can provide @{link ThrottlePolicy}s.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface ThrottlePolicyProvider {
    List<ThrottlePolicy> getThrottlePolicies();
}
