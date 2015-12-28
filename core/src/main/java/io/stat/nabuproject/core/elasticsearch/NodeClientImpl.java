package io.stat.nabuproject.core.elasticsearch;

import com.google.inject.Inject;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.nabu.NabuConfig;
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

    private NabuConfig config;

    @Inject
    NodeClientImpl(NabuConfig configProvider) {
        this.config = configProvider;

        nodeSettingsBuilder = Settings.settingsBuilder()
                .put("path.home", config.getESHome())
                .put("http.enabled", config.isESHTTPEnabled()) // maybe serve HTTP requests
                .put("http.port", config.getESHTTPPort()) // maybe serve HTTP requests
                .put("node.master", false)
                .put("node.nabuproject", true);

        nodeBuilder = NodeBuilder.nodeBuilder()
                .settings(nodeSettingsBuilder)
                .clusterName(config.getESClusterName())
                .data(false)
                .local(false)
                .client(true);

        this.esNode = this.nodeBuilder.build();
    }

    @Override
    public void start() throws ComponentException {
        try {
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
