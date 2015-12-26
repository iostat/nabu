package io.stat.nabu.elasticsearch;

import com.google.inject.Inject;
import io.stat.nabu.config.ConfigurationProvider;
import io.stat.nabu.core.ComponentException;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

/**
 * Created by io on 12/25/15. (929) 253-6977 $50/hr
 */
@Slf4j
class NodeClientImpl extends ESClient {
    private Settings.Builder nodeSettingsBuilder;
    private NodeBuilder nodeBuilder;

    private Node esNode;
    private Client esNodeClient;

    private ConfigurationProvider config;

    @Inject
    NodeClientImpl(ConfigurationProvider config) {
        this.config = config;

        nodeSettingsBuilder = Settings.settingsBuilder()
                .put("path.home", config.getESHome())
                .put("http.enabled", config.isESHTTPEnabled()) // don't serve HTTP requests!
                .put("http.port", config.getESHTTPPort()); // don't serve HTTP requests!
        nodeBuilder = NodeBuilder.nodeBuilder()
                .settings(nodeSettingsBuilder)
                .data(false)
                .local(false)
                .client(true);
    }

    @Override
    public void start() throws ComponentException {
        try {
            this.esNode = this.nodeBuilder.build();
            this.esNode.start();
        } catch (Exception e) {
            logger.error("Could not create the Nabu ES node", e);
            throw new ComponentException(true, e);
        }

        this.esNodeClient = this.esNode.client();
    }

    @Override
    public void shutdown() throws ComponentException {
        this.esNode.close();
    }
}
