package io.stat.nabuproject.nabu.kafka;

import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.nabu.NabuConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by io on 12/26/15. (929) 253-6977 $50/hr
 */
@RequiredArgsConstructor
@Slf4j
class KafkaProducerImpl extends NabuKafkaProducer {
    private NabuConfig config;

    @Override
    public void start() throws ComponentException {

    }

    @Override
    public void shutdown() throws ComponentException {

    }
}
