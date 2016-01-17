package io.stat.nabuproject.enki.integration;

import com.google.inject.Inject;
import io.stat.nabuproject.core.Component;
import io.stat.nabuproject.core.DynamicComponent;
import io.stat.nabuproject.core.throttling.ThrottlePolicy;
import io.stat.nabuproject.core.throttling.ThrottlePolicyProvider;
import io.stat.nabuproject.enki.leader.LeaderEventSource;

import java.util.List;

/**
 * Pulls throttle policy information from ZooKeeper, and if this node is the leader,
 * creates it if it didn't exist already.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
class ZKThrottlePolicyProvider extends Component implements ThrottlePolicyProvider {
    private final DynamicComponent<ThrottlePolicyProvider> replacer;
    private final LeaderEventSource leaderEventSource;

    @Inject
    public ZKThrottlePolicyProvider(DynamicComponent<ThrottlePolicyProvider> replacer,
                                    LeaderEventSource leaderEventSource) {
        this.replacer = replacer;
        this.leaderEventSource = leaderEventSource;
    }

    @Override
    public List<ThrottlePolicy> getThrottlePolicies() {
        return replacer.getCurrentInstance()
                .getThrottlePolicies();
    }
}
