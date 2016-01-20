package io.stat.nabuproject.core.telemetry;

import io.stat.nabuproject.core.Component;

/**
 * An abstract telemetry service.
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public abstract class TelemetryService extends Component {
    public abstract TelemetryCounterSink createCounter(String aspectName, String... tags);
    public abstract TelemetryGaugeSink createGauge(String aspectName, String... tags);
    public abstract TelemetryGaugeSink createExecTime(String aspectName, String... tags);
}
