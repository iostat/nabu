package io.stat.nabuproject.core.elasticsearch;

import java.util.Map;

/**
 * Provides configuration to the Elasticsearch module
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface ESConfigProvider {
    String getESHome();
    String getESClusterName();
    boolean isESHTTPEnabled();
    int getESHTTPPort();

    Map<String, String> getESNodeAttributes();
    Map<String, String> getAdditionalESProperties();
}
