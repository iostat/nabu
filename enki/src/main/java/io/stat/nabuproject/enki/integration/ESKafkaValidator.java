package io.stat.nabuproject.enki.integration;

import io.stat.nabuproject.core.Component;

import java.util.Map;

/**
 * Something which can validate the
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public abstract class ESKafkaValidator extends Component {
    public abstract boolean isSane();
    abstract Map<String, ESKSP> getShardCountCache();
}
