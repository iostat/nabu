package io.stat.nabuproject.core.elasticsearch;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.elasticsearch.event.NabuESEvent;
import io.stat.nabuproject.core.elasticsearch.event.NabuESEventListener;
import io.stat.nabuproject.core.net.AddressPort;
import io.stat.nabuproject.core.util.Tuple;
import io.stat.nabuproject.core.util.concurrent.NamedThreadFactory;
import io.stat.nabuproject.core.util.dispatch.AsyncListenerDispatcher;
import io.stat.nabuproject.core.util.dispatch.ShutdownOnFailureCRC;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static io.stat.nabuproject.core.util.functional.FluentCompositions.curry2p;

/**
 * An implementation of {@link ESClient} which is backed by an ElasticSearch NodeClient
 */
@Slf4j @EqualsAndHashCode(callSuper=true)
class NodeClientImpl extends ESClient {
    private static final Supplier<ESException> NOT_READY_SUPPLIER =
            () -> new ESException(
                    "The ESClient is not ready to process this request");

    private static final Supplier<ESException> INVALID_METADATA_RESPONSE_SUPPLIER =
            () -> new ESException(
                "Metadata request completed successfully, " +
                "but the response did not have information " +
                "for the requested index");

    private static final Supplier<ESException> NO_SHARD_COUNT_SUPPLIER =
            () -> new ESException(
                    "The metadata response of this index did not " +
                    "contain a shard count for the requested index");

    /**
     * How long to wait for the ES Node to start. Should probably
     * be configurable.
     */
    private static final long ES_START_TIMEOUT = 120000;

    private Settings.Builder nodeSettingsBuilder;
    private NodeBuilder nodeBuilder;

    private Node esNode;
    private @Getter Client eSClient;
    private ClusterService esNodeClusterService;
    private ClusterState clusterState;
    private final ClusterStateListener clusterStateListener;

    private ESConfigProvider config;

    private final byte[] $clusterStateLock;

    private final AsyncListenerDispatcher<NabuESEventListener> dispatcher;

    private final Injector injector;

    /**
     * Sort of a hack... we create a new thread in this factory
     * to start ElasticSearch so that all of ES's threads will be their
     * own thread group, so as not to pollute my debugger.
     */
    private final NamedThreadFactory esThreadFactory;

    @Inject
    NodeClientImpl(ESConfigProvider configProvider, Injector injector) {
        this.config = configProvider;
        this.injector = injector;

        this.clusterStateListener = this.new ClusterStateListener();

        this.$clusterStateLock = new byte[0];

        this.dispatcher = new AsyncListenerDispatcher<>("NabuESEvent");
        this.esThreadFactory = new NamedThreadFactory("ES Node");
    }

    @Override
    public void start() throws ComponentException {
        try {

            Thread esStarterThread = esThreadFactory.newThread(() -> {
                synchronized (NodeClientImpl.this) {
                    nodeSettingsBuilder = Settings.settingsBuilder()
                            .put("path.home", config.getESHome())
                            .put("http.enabled", config.isESHTTPEnabled()) // maybe serve HTTP requests
                            .put("http.port", config.getESHTTPPort()) // maybe serve HTTP requests
                            .put("node.master", false);

                    config.getESNodeAttributes().forEach((k, v) -> nodeSettingsBuilder.put("node."+ k, v));

                    // need to coer
                    config.getAdditionalESProperties().forEach((k, v) -> nodeSettingsBuilder.put(k, v));

                    nodeBuilder = NodeBuilder.nodeBuilder()
                            .settings(nodeSettingsBuilder)
                            .clusterName(config.getESClusterName())
                            .data(false)
                            .local(false)
                            .client(true);

                    this.esNode = this.nodeBuilder.build();

                    esNode.start();
                }
            });
            esStarterThread.start();
            esStarterThread.join(ES_START_TIMEOUT);
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
        this.dispatcher.shutdown();
        this.esNode.close();
    }


    @Override @Synchronized
    public boolean isEnkiDiscovered() {
        return getDiscoveredEnkis().size() > 0;

    }

    @Override @Synchronized
    public List<Tuple<String,AddressPort>> getDiscoveredEnkis() {
        ImmutableList.Builder<Tuple<String,AddressPort>> builder = ImmutableList.builder();

        for(DiscoveryNode n : this.clusterState.getNodes()) {
            if(nodeIsEnkiNode(n)) {
                String[] addressBits = n.getAttributes().getOrDefault("enki", "").split(":");
                builder.add(
                        new Tuple<>(
                            n.name(),
                            new AddressPort(addressBits[0], Integer.parseInt(addressBits[1]))
                        )
                );
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
    public String getESIdentifier() {
        if(this.esNode != null) {
            return this.esNode.settings().get("name");
        }

        return "NOT AVAILABLE";
    }

    @Override
    public GetIndexResponse getIndexMetadata(String... indices) throws ESException{
        Client client = Optional.of(getESClient())
                                  .orElseThrow(NOT_READY_SUPPLIER);
        try {
            return client
                    .admin()
                    .indices()
                    .prepareGetIndex().setIndices(indices)
                    .get();

        } catch (Exception e) {
            throw new ESException(e);
        }
    }

    @Override
    public int getShardCount(String indexName) throws ESException {
        GetIndexResponse resp = getIndexMetadata(indexName);

        Settings indexSettings = Optional.of(resp.getSettings().getOrDefault(indexName, null))
                                         .orElseThrow(INVALID_METADATA_RESPONSE_SUPPLIER);

        String shardCountStr = Optional.of(indexSettings.get(INDEX_NUMBER_OF_PRIMARY_SHARDS_SETTING, null))
                                       .orElseThrow(NO_SHARD_COUNT_SUPPLIER);

        int ret;
        try {
            ret = Integer.parseInt(shardCountStr, 10);
        } catch(Exception e) {
            throw new ESException("Error while parsing the number of shards returned by ElasticSearch (got: \"" + shardCountStr + "\")", e);
        }
        return ret;
    }

    @Override
    public void addNabuESEventListener(NabuESEventListener listener) {
        dispatcher.addListener(listener);
    }

    @Override
    public void removeNabuESEventListener(NabuESEventListener listener) {
        dispatcher.removeListener(listener);
    }

    private void dispatchNabuEsEvent(NabuESEvent event) {
        dispatcher.dispatchListenerCallbacks(curry2p(NabuESEventListener::onNabuESEvent, event),
                new ShutdownOnFailureCRC(this, "NabuESEventCallbackFailedCRC"));
    }

    private void dispatchClusterHealthChange(ClusterHealthStatus color) {
        dispatcher.dispatchListenerCallbacks(curry2p(NabuESEventListener::onClusterColorChange, color),
                new ShutdownOnFailureCRC(this, "NabuESEventCallbackFailedCRC"));
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
            synchronized(NodeClientImpl.this.$clusterStateLock) {
                NodeClientImpl.this.clusterState = event.state();
            }
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

            ClusterHealthStatus theColor = ClusterHealthStatus.GREEN;
            for(ShardRouting shardRouting : event.state().getRoutingTable().allShards()) {
                if(!shardRouting.started()) {
                    theColor = ClusterHealthStatus.YELLOW;
                    if(shardRouting.primary()) {
                        theColor = ClusterHealthStatus.RED;
                        break;
                    }
                }
            }
            NodeClientImpl.this.dispatchClusterHealthChange(theColor);
        }
    }
}
