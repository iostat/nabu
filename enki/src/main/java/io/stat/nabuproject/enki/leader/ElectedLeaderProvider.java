package io.stat.nabuproject.enki.leader;

import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static io.stat.nabuproject.core.util.functional.FluentCompositions.on;

/**
 * Something which can provide information on who the leader is amongst a group of Enkis.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface ElectedLeaderProvider {
    /**
     * @return Whether or not this instance is the currently elected leader
     */
    boolean isSelf();

    /**
     * @return A list of all nodes who are eligible to be leaders, including this one, if applicable.
     */
    List<LeaderData> getLeaderCandidates();

    /**
     * Get the leader data that this provider advertises itself with.
     */
    LeaderData getOwnLeaderData();

    /**
     * Get the data of the actual leader.
     */

    default LeaderData getElectedLeaderData() {
        Logger logger = LoggerFactory.getLogger(ElectedLeaderProvider.class);
        logger.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        logger.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        logger.error("!!!!! USING THE DEFAULT IMPL of getElectedLeaderData !!!!!");
        logger.error("!!!!! THIS IS BEYOND WRONG, THIS SHOULD NEVER HAPPEN !!!!!");
        logger.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        logger.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

        return isSelf() ? getOwnLeaderData() : getLeaderCandidates().stream()
                                                                    .sorted(on(LeaderData::getPriority, Longs::compare))
                                                                    .findFirst()
                                                                    .orElse(getOwnLeaderData());
    }
}
