package io.stat.nabuproject.core.telemetry;

/**
 * Something which can send counters to telemetry sources.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface TelemetryCounterSink extends TelemetrySink {
    void increment();
    void decrement();
}
