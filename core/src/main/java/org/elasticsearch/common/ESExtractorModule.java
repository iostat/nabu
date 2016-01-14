package org.elasticsearch.common;

import com.google.inject.AbstractModule;
import lombok.experimental.Delegate;

/**
 * A Guice module that allows us to bind org.elasticsearch.common
 * package-private instances in a Guice-injectable manner.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class ESExtractorModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(ESTimeBasedUUIDGen.class).to(TimeBasedUUIDGenProxy.class);
    }

    /**
     * A proxy class to the package-private org.elasticsearch.TimeBasedUUIDGenerator
     */
    public static class TimeBasedUUIDGenProxy implements ESTimeBasedUUIDGen {
        @Delegate(types=ESTimeBasedUUIDGen.class) private final TimeBasedUUIDGenerator tbug;

        public TimeBasedUUIDGenProxy() {
            this.tbug = new TimeBasedUUIDGenerator();
        }
    }
}
