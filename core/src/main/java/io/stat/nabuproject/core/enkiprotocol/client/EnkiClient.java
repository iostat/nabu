package io.stat.nabuproject.core.enkiprotocol.client;

import io.stat.nabuproject.core.Component;
import io.stat.nabuproject.core.enkiprotocol.dispatch.EnkiClientEventSource;
import io.stat.nabuproject.core.kafka.KafkaBrokerConfigProvider;
import io.stat.nabuproject.core.net.AddressPort;
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

    /**
     * Called by a EnkiConnection when it is told to redirect.
     * @param ap the AddressPort of the redirection target.
     */
    abstract void setRedirectionTarget(AddressPort ap);

    /**
     * Called in the extreme circumstance that an inexplicable exception happened in the protocol.
     * For the safety of everyone, this should perform an action that causes the entire Nabu
     * instance to shut down.
     */
    abstract void shutDownEverything();
}
