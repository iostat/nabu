package io.stat.nabuproject.core.elasticsearch.event;

import lombok.Value;
import org.elasticsearch.cluster.node.DiscoveryNode;

/**
 * Represents an event that happens in the ElasticSearch cluster membership
 * that affects Nabu/Enki in some way.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Value
public class NabuESEvent {
    Type type;
    DiscoveryNode node;

    /**
     * Represents an event in the elasticsearch cluster that involves Nabu/Enki nodes.
     *
     * @author Ilya Ostrovskiy (https://github.com/iostat/)
     */
    public enum Type {
        ENKI_JOINED,
        ENKI_PARTED,
        NABU_JOINED,
        NABU_PARTED
    }
}
