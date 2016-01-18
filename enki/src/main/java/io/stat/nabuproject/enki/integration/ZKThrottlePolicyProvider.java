package io.stat.nabuproject.enki.integration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.stat.nabuproject.core.Component;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.DynamicComponent;
import io.stat.nabuproject.core.throttling.ThrottlePolicy;
import io.stat.nabuproject.core.throttling.ThrottlePolicyChangeListener;
import io.stat.nabuproject.core.throttling.ThrottlePolicyProvider;
import io.stat.nabuproject.core.util.dispatch.AsyncListenerDispatcher;
import io.stat.nabuproject.core.util.dispatch.ShutdownOnFailureCRC;
import io.stat.nabuproject.enki.Enki;
import io.stat.nabuproject.enki.zookeeper.ZKClient;
import io.stat.nabuproject.enki.zookeeper.ZKEventListener;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Pulls throttle policy information from ZooKeeper, and if this node is the leader,
 * creates it if it didn't exist already.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
class ZKThrottlePolicyProvider extends Component implements ThrottlePolicyProvider, ZKEventListener {
    public static final String TP_SUBNODE = "/policies";

    private final AsyncListenerDispatcher<ThrottlePolicyChangeListener> dispatcher;

    @Override
    public boolean onZkDataChange(ZKClient sender, String nodePath, String newData) {
        logger.info("data change in {} => {}", nodePath, newData);
        String tpName = nodePath.replaceFirst(TP_SUBNODE + "/", "");
        createOrUpdateTPRef(tpName, newData);

        dispatcher.dispatchListenerCallbacks(ThrottlePolicyChangeListener::onThrottlePolicyChange,
                new ShutdownOnFailureCRC(injector.getInstance(Enki.class), "ZKTPPChange"));

        return true;
    }

    private final List<AtomicReference<ThrottlePolicy>> tpList;

    public static final String ENCODE_ZKTP(String kafkaTopicName, int batchSize, long targetTime) {
        return String.format("%s:%d,%d", kafkaTopicName, batchSize, targetTime);
    }

    private final DynamicComponent<ThrottlePolicyProvider> replacer;
    private final ZKClient zkClient;
    private final Injector injector;

    @Override
    public void shutdown() throws ComponentException {
        this.dispatcher.shutdown();
    }

    @Inject
    public ZKThrottlePolicyProvider(DynamicComponent<ThrottlePolicyProvider> replacer,
                                    ZKClient zkClient,
                                    Injector injector) {
        this.replacer = replacer;
        this.zkClient = zkClient;
        this.injector = injector;
        this.tpList   = Lists.newArrayList();
        this.dispatcher = new AsyncListenerDispatcher<>("ZKTPP-ChangeDispatcher");
        logger.info("Constructed ZKTPP!");
    }

    @Override
    public List<AtomicReference<ThrottlePolicy>> getTPReferences() {
        logger.info("GetTPP!");
        return ImmutableList.copyOf(tpList);
    }

    @Override
    public void registerThrottlePolicyChangeListener(ThrottlePolicyChangeListener tpcl) {
        logger.info("registering TPCL {}", tpcl);
        dispatcher.addListener(tpcl);
    }

    @Override
    public void deregisterThrottlePolicyChangeListener(ThrottlePolicyChangeListener tpcl) {
        logger.info("removing TPCL {}", tpcl);
        dispatcher.removeListener(tpcl);
    }

    @Override
    public Collection<ThrottlePolicyChangeListener> getAllTPCLs() {
        return dispatcher.getListeners();
    }

    @Override
    public void seedTPCLs(Collection<ThrottlePolicyChangeListener> tpcls) {
        logger.info("Migrating TPCLs from previous listener: {}", tpcls);
        tpcls.forEach(dispatcher::addListener);
    }

    @Synchronized
    void seedTPs(Map<String, String> tps) {
        tps.forEach((k, v) -> {
            logger.info("Subscribing to {}", k);
            String path = encodeTPPath(k);
            String data = zkClient.getAndSubscribe(path, this);
            createOrUpdateTPRef(k, data);
        });
    }

    @Synchronized
    void createOrUpdateTPRef(String tpName, String data) {
        // todo: what if corrupt data gets put into ZK
        // todo: currently no real way to handle that.
        ThrottlePolicy merged = mergeDataToRootTP(tpName, data);
        for(AtomicReference<ThrottlePolicy> currTPRef : tpList) {
            if(currTPRef.get().getIndexName().equals(tpName)) {
                currTPRef.set(merged);
                logger.info("Merged TPRef containing {}", merged);
                return;
            }
        }

        logger.info("Registering new (local) TPRef with {}", merged);
        tpList.add(new AtomicReference<>(merged));
    }

    ThrottlePolicy mergeDataToRootTP(String tpName, String data) {
        // todo: technically there's no way that the ZKTPValidator would let it
        // todo: get this far, but for the sake of defensive programming
        // todo: we need a fallback in case the default TPP doesn't have configs
        // todo: for the index.
        List<ThrottlePolicy> rootTPs = replacer.getDefaultInstance().getThrottlePolicies();
        ThrottlePolicy target = null;
        for(ThrottlePolicy tp : rootTPs) {
            if(tp.getIndexName().equals(tpName)) {
                target = tp;
                break;
            }
        }

        if(target == null) {
            throw new RuntimeException("Somehow did not have a corresponding TP for " + tpName + ". This is impossible.");
        }

        // todo: validate the data we get out of ZK.
        String[] rootBits = data.split(":");
        String kafkaName = rootBits[0];
        String[] numberBits = rootBits[1].split(",");

        int batchSize = Integer.parseInt(numberBits[0]);
        long targetMS = Long.parseLong(numberBits[1]);

        return new ThrottlePolicy(tpName, kafkaName, targetMS, batchSize, target.getFlushTimeout());
    }

    static String encodeTPPath(String tpName) {
        return TP_SUBNODE + "/" + tpName;
    }
}
