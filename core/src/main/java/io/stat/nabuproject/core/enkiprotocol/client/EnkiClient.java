package io.stat.nabuproject.core.enkiprotocol.client;

import io.stat.nabuproject.core.Component;
import io.stat.nabuproject.core.enkiprotocol.dispatch.EnkiClientEventSource;
import io.stat.nabuproject.core.kafka.KafkaBrokerConfigProvider;
import io.stat.nabuproject.core.throttling.ThrottlePolicyProvider;

/**
 * Created by io on 1/3/16. io is an asshole because
 * he doesn't write documentation for his code.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public abstract class EnkiClient extends Component implements
        KafkaBrokerConfigProvider,
        ThrottlePolicyProvider,
        EnkiClientEventSource {
}
