package io.stat.nabuproject.core.telemetry;

/**
 * Something which can provide configuration for telemetry services.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface TelemetryConfigProvider {
    String getTelemetryPrefix();

    String getTelemetryServer();

    int getTelemetryPort();
}
