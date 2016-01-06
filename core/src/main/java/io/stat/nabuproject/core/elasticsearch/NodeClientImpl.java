package io.stat.nabuproject.core.elasticsearch;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.elasticsearch.event.NabuESEvent;
import io.stat.nabuproject.core.net.AddressPort;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.util.List;

/**
 * An implementation of {@link ESClient} which is backed by an ElasticSearch NodeClient
 */
@Slf4j @EqualsAndHashCode(callSuper=true)
class NodeClientImpl extends ESClient {
    private Settings.Builder nodeSettingsBuilder;
    private NodeBuilder nodeBuilder;

    private Node esNode;
    private @Getter Client eSClient;
    private ClusterService esNodeClusterService;
    private ClusterState clusterState;
    private final ClusterStateListener clusterStateListener;

    private ESConfigProvider config;

    @Inject
    NodeClientImpl(ESConfigProvider configProvider) {
        this.config = configProvider;

        nodeSettingsBuilder = Settings.settingsBuilder()
                .put("path.home", config.getESHome())
                .put("http.enabled", config.isESHTTPEnabled()) // maybe serve HTTP requests
                .put("http.port", config.getESHTTPPort()) // maybe serve HTTP requests
                .put("node.master", false);

        configProvider.getESNodeAttributes().forEach((k, v) -> nodeSettingsBuilder.put("node."+ k, v));

        nodeBuilder = NodeBuilder.nodeBuilder()
                .settings(nodeSettingsBuilder)
                .clusterName(config.getESClusterName())
                .data(false)
                .local(false)
                .client(true);

        this.esNode = this.nodeBuilder.build();
        this.clusterStateListener = this.new ClusterStateListener();
    }

    @Override
    public void start() throws ComponentException {
        try {
            this.esNode.start();
        } catch (Exception e) {
            logger.error("Could not create the Nabu ES node", e);
            throw new ComponentException(true, e);
        }

        this.esNodeClusterService = this.esNode.injector().getInstance(ClusterService.class);
        this.clusterState = esNodeClusterService.state();
        this.eSClient = this.esNode.client();
        this.esNodeClusterService.add(clusterStateListener);
    }

    @Override
    public void shutdown() throws ComponentException {
        this.esNodeClusterService.remove(clusterStateListener);
        this.esNode.close();
    }


    @Override @Synchronized
    public boolean isEnkiDiscovered() {
        return getDiscoveredEnkis().size() > 0;

    }

    @Override @Synchronized
    public List<AddressPort> getDiscoveredEnkis() {
        ImmutableList.Builder<AddressPort> builder = ImmutableList.builder();

        for(DiscoveryNode n : this.clusterState.getNodes()) {
            if(nodeIsEnkiNode(n)) {
                String[] addressBits = n.getAttributes().getOrDefault("enki", "").split(":");
                builder.add(new AddressPort(addressBits[0], Integer.parseInt(addressBits[1])));
            }
        }


        return builder.build();
    }

    private static boolean nodeIsEnkiNode(DiscoveryNode n) {
        return !Strings.isEmpty(n.attributes().getOrDefault("enki", ""));
    }

    private static boolean nodeIsNabuNode(DiscoveryNode n) {
        return n.attributes().getOrDefault("nabu", "false").equals("true");
    }

    @Override
    public String getElasticSearchIndentifier() {
        if(this.esNode != null) {
            return this.esNode.settings().get("name");
        }

        return "NOT AVAILABLE";
    }

    /**
     * Here so we can register with the node's cluster state listener API
     * and dispatch our own events.
     */
    @EqualsAndHashCode
    private final class ClusterStateListener implements org.elasticsearch.cluster.ClusterStateListener {
        /**
         * @param event the event that ElasticSearch sent us
         */
        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            List<DiscoveryNode> addedNodes   = event.nodesDelta().addedNodes();
            List<DiscoveryNode> removedNodes = event.nodesDelta().removedNodes();

            addedNodes.forEach(node -> {
                boolean isNabu = nodeIsNabuNode(node);
                boolean isEnki = nodeIsEnkiNode(node);

                if(isEnki && isNabu) {
                    logger.warn("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                    logger.warn("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                    logger.warn("!!!!! a node claiming to be both an Enki and a Nabu has joined the cluster. !!!!!");
                    logger.warn("!!!!!         please review your system, as this should never happen        !!!!!");
                    logger.warn("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                    logger.warn("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                } else if (isEnki) {
                    logger.info("An Enki has joined the cluster: {}", node);
                    NodeClientImpl.this.dispatchNabuEsEvent(new NabuESEvent(NabuESEvent.Type.ENKI_JOINED, node));
                } else if(isNabu) {
                    logger.info("A Nabu has joined the cluster: {}", node);
                    NodeClientImpl.this.dispatchNabuEsEvent(new NabuESEvent(NabuESEvent.Type.NABU_JOINED, node));
                } else {
                    logger.info("Non Enki/Nabu node...: {}", node);
                }
            });

            removedNodes.forEach(node -> {
                boolean isNabu = nodeIsNabuNode(node);
                boolean isEnki = nodeIsEnkiNode(node);

                if(isEnki && isNabu) {
                    logger.warn("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                    logger.warn("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                    logger.warn("!!!!!      a node claiming to be both an Enki and a Nabu has left the cluster.      !!!!!");
                    logger.warn("!!!!! this should never happen (although since it left thats probably a good thing) !!!!!");
                    logger.warn("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                    logger.warn("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                } else if (isEnki) {
                    logger.info("An Enki has left the cluster: {}", node);
                    NodeClientImpl.this.dispatchNabuEsEvent(new NabuESEvent(NabuESEvent.Type.ENKI_PARTED, node));
                } else if(isNabu) {
                    logger.info("A Nabu has left the cluster: {}", node);
                    NodeClientImpl.this.dispatchNabuEsEvent(new NabuESEvent(NabuESEvent.Type.NABU_PARTED, node));
                } else {
                    logger.info("Non Enki/Nabu node...: {}", node);
                }
            });
        }

    }
}
