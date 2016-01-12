package io.stat.nabuproject.nabu.kafka;

import io.stat.nabuproject.core.Component;
import io.stat.nabuproject.core.enkiprotocol.dispatch.EnkiClientEventListener;

/**
 * An abstract class which describes something which listens to EnkiClientEvents, and
 * coordinates KafkaConsumer sets for assigned topics.
 */
public abstract class AssignedConsumptionCoordinator extends Component implements EnkiClientEventListener {
}
