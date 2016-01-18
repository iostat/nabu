package io.stat.nabuproject.core.telemetry;

/**
 * Created by io on 1/18/16. io is an asshole because
 * he doesn't write documentation for his code.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface TelemetrySink {
    String getAspect();
    String getPrefix();
    String[] getTags();
}
