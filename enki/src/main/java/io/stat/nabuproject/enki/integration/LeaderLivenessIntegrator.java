package io.stat.nabuproject.enki.integration;

import io.stat.nabuproject.core.Component;
import io.stat.nabuproject.enki.leader.ElectedLeaderProvider;

/**
 * An Abstract class which describes a component that integrates multiple cluster state
 * data sources, whose update timings are unsynchronized into one cohesive state.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public abstract class LeaderLivenessIntegrator extends Component implements ElectedLeaderProvider {
}
