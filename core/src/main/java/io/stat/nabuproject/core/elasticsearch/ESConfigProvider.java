package io.stat.nabuproject.core.elasticsearch;

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
}
