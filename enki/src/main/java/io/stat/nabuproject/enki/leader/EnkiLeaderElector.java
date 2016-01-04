package io.stat.nabuproject.enki.leader;

import io.stat.nabuproject.core.Component;

/**
 * The Component which performs leader election and gets information of leaders.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public abstract class EnkiLeaderElector extends Component implements ElectedLeaderProvider, LeaderEventSource {
}
