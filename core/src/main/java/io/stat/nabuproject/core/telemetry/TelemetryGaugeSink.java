package io.stat.nabuproject.core.telemetry;

/**
 * Something which can send "constant" values/rates to the telemetry backend
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface TelemetryGaugeSink extends TelemetrySink {
    void set(long value);

}
