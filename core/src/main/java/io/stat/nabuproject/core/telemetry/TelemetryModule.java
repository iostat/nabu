package io.stat.nabuproject.core.telemetry;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

/**
 * Guice module for telemetry services.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class TelemetryModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(TelemetryImpl.class).in(Singleton.class);
        bind(TelemetryService.class).to(TelemetryImpl.class);
    }
}
