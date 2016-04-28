package io.stat.nabuproject.core.telemetry;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import io.stat.nabuproject.core.ComponentException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The canonical implementation of {@link TelemetryService}
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
class TelemetryImpl extends TelemetryService {
    private final TelemetryConfigProvider config;
    private final List<StatsDClient> allocatedClients;

    @Inject
    public TelemetryImpl(TelemetryConfigProvider config) {
        this.config = config;
        this.allocatedClients = Lists.newArrayList();
    }

    @Override
    public TelemetryCounterSink createCounter(String aspectName, String... tags) {
        synchronized(allocatedClients) {
            logger.info("Creating telemetry backend for counter:{}.{}({})", config.getTelemetryPrefix(), aspectName, tags);
            NonBlockingStatsDClient cli = makeClient();
            CounterImpl ret = new CounterImpl(config.getTelemetryPrefix(), aspectName, tags, cli);
            allocatedClients.add(cli);
            return ret;
        }
    }

    @Override
    public TelemetryGaugeSink createGauge(String aspectName, String... tags) {
        synchronized(allocatedClients) {
            logger.info("Creating telemetry backend for gauge:{}.{}({})", config.getTelemetryPrefix(), aspectName, tags);
            NonBlockingStatsDClient cli = makeClient();
            GaugeImpl ret = new GaugeImpl(config.getTelemetryPrefix(), aspectName, tags, cli);
            allocatedClients.add(cli);
            return ret;
        }
    }

    @Override
    public TelemetryGaugeSink createExecTime(String aspectName, String... tags) {
        logger.info("Creating telemetry backend for execTime:{}.{}({})", config.getTelemetryPrefix(), aspectName, tags);
        NonBlockingStatsDClient cli = makeClient();
        ExecTimeImpl ret = new ExecTimeImpl(config.getTelemetryPrefix(), aspectName, tags, cli);
        allocatedClients.add(cli);
        return ret;
    }

    @Override
    public void shutdown() throws ComponentException {
        logger.info("Shutting down telemetry backends...");
        int total = allocatedClients.size();
        AtomicInteger thisOne = new AtomicInteger(0);
        allocatedClients.parallelStream().forEach((client) -> {
            logger.info("Stopping telemetry backend: {}/{}", thisOne.incrementAndGet(), total);
            client.stop();
        });
        logger.info("Stopped all telemetry backends");
    }

    private NonBlockingStatsDClient makeClient() {
        return new NonBlockingStatsDClient(config.getTelemetryPrefix(), config.getTelemetryServer(), config.getTelemetryPort());
    }

    @RequiredArgsConstructor
    private static final class CounterImpl implements TelemetryCounterSink {
        private final @Getter String prefix;
        private final @Getter String aspect;
        private final @Getter String[] tags;
        private final StatsDClient client;

        @Override
        public void increment() {
            client.increment(aspect, tags);
        }

        @Override
        public void decrement() {
            client.decrement(aspect, tags);
        }

        @Override
        public void delta(long amt) {
            client.count(aspect, amt, tags);
        }
    }

    @RequiredArgsConstructor
    private static final class GaugeImpl implements TelemetryGaugeSink {
        private final @Getter String prefix;
        private final @Getter String aspect;
        private final @Getter String[] tags;
        private final StatsDClient client;

        @Override
        public void set(long value) {
            client.gauge(aspect, value, tags);
        }
    }

    @RequiredArgsConstructor
    private static final class ExecTimeImpl implements TelemetryGaugeSink {
        private final @Getter String prefix;
        private final @Getter String aspect;
        private final @Getter String[] tags;
        private final StatsDClient client;

        @Override
        public void set(long ms) {
            client.time(aspect, ms, tags);
        }
    }
}
