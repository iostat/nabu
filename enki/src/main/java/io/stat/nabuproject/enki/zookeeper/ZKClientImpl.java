package io.stat.nabuproject.enki.zookeeper;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.kafka.KafkaZKStringSerializerProxy;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.zookeeper.CreateMode;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static io.stat.nabuproject.core.util.functional.FluentCompositions.curry2;

/**
 * The canonical implementation of the ZooKeeper client component
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
class ZKClientImpl extends ZKClient {
    private final ZKConfigProvider config;
    private ZkClient backingClient;
    private final CountDownLatch startLatch;
    private final List<ZKChildListenerProxy> childListeners;
    private final List<ZKDataListenerProxy> dataListeners;
    private final AtomicBoolean isShuttingDown;

    @Inject
    public ZKClientImpl(ZKConfigProvider config) {
        this.config = config;
        this.startLatch = new CountDownLatch(1);

        this.childListeners = Lists.newArrayList();
        this.dataListeners  = Lists.newArrayList();

        this.isShuttingDown = new AtomicBoolean(false);
    }

    @Synchronized
    private void assertStart() {
        try {
            startLatch.await();
            throwShuttingDownIfNecessary();
        } catch(InterruptedException e) {
            logger.error("Interrupted while waiting for ZK to start. This will get nasty.", e);
        }
    }

    private void throwShuttingDownIfNecessary() {
        if(isShuttingDown.get()) {
            throw new RuntimeException("The ZK Client is shutting down and cannot service requests");
        }
    }

    @Override @Synchronized
    public void start() throws ComponentException {
        Function<String, String> appendChroot = curry2(String::concat, config.getZKChroot());
        try {
            Iterator<String> chrootedZookeepersIterator =
                    config.getZookeepers()
                            .stream()
                            .map(appendChroot)
                            .iterator();

            this.backingClient = new ZkClient(
                    new ZkConnection(Joiner.on(',').join(chrootedZookeepersIterator)),
                    config.getZKConnectionTimeout(),
                    new KafkaZKStringSerializerProxy());

            int childCountOfRoot = backingClient.countChildren("/");
            logger.info("Counted {} in Nabu ZK chroot.", childCountOfRoot);
            startLatch.countDown();
        } catch(Exception e) {
            throw new ComponentException(true, "Failed to start the ZK leader election system", e);
        }
    }

    @Override @Synchronized
    public void shutdown() throws ComponentException {
        this.isShuttingDown.set(true);
        startLatch.countDown(); // so we dont deadlock when shutting down.
        if(this.backingClient != null) {
            this.backingClient.close();
        }
    }

    @Override @Synchronized
    public String getAndSubscribe(String path, ZKEventListener subscriber) {
        assertStart();
        ZKDataListenerProxy p = new ZKDataListenerProxy(subscriber, this, path);
        dataListeners.add(p);
        String data = get(path);
        backingClient.subscribeDataChanges(path, p);
        return data;
    }

    @Override @Synchronized
    public List<String> getChildrenAndSubscribe(String path, ZKEventListener subscriber) {
        assertStart();
        ZKChildListenerProxy p = new ZKChildListenerProxy(subscriber, this, path);
        childListeners.add(p);
        return backingClient.subscribeChildChanges(path, p);
    }

    @Override @Synchronized
    public void unsubscribeFromChildEvents(String path, ZKEventListener listener) {
        childListeners.removeIf(p -> p.unsubscribeIfIsListenerFor(path, listener, backingClient));
    }

    @Override @Synchronized
    public void unsubscribeFromDataEvents(String path, ZKEventListener listener) {
        dataListeners.removeIf(p -> p.unsubscribeIfIsListenerFor(path, listener, backingClient));
    }

    @Override @Synchronized
    public void unsubscribeFromAllEvents(ZKEventListener listener) {
        childListeners.removeIf(p -> p.unsubscribeIfIsListenerFor(p.getPath(), listener, backingClient));
        dataListeners.removeIf(p -> p.unsubscribeIfIsListenerFor(p.getPath(), listener, backingClient));
    }


    @Override
    public String get(String path) {
        assertStart();
        logger.info("get {}", path);
        return backingClient.readData(path, true);
    }

    @Override
    public List<String> getChildren(String path) {
        assertStart();
        logger.info("getChildren {}", path);
        return backingClient.getChildren(path);
    }

    @Override
    public void write(String path, String data) {
        logger.info("Writing {} to {}", data, path);
        backingClient.writeData(path, data);
    }

    @Override
    public void create(String path, String data) {
        logger.info("Creating {} with {}", path, data);
        backingClient.create(path, data, CreateMode.PERSISTENT);
    }

    @EqualsAndHashCode @RequiredArgsConstructor
    private final static class ZKChildListenerProxy implements IZkChildListener {
        private final ZKEventListener toProxy;
        private final ZKClientImpl zkClient;
        private final @Getter String path;

        @Override
        public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
            toProxy.onZKPathChange(zkClient, parentPath, currentChilds);
        }

        boolean unsubscribeIfIsListenerFor(String path, ZKEventListener listener, ZkClient client) {
            // yes, reference equality.
            boolean ret = path.equals(getPath()) && listener == this.toProxy;
            if(ret) {
                client.unsubscribeChildChanges(path, this);
            }

            return ret;
        }
    }

    @EqualsAndHashCode @RequiredArgsConstructor
    private final static class ZKDataListenerProxy implements IZkDataListener {
        private final ZKEventListener toProxy;
        private final ZKClientImpl zkClient;
        private final @Getter String path;

        @Override
        public void handleDataChange(String dataPath, Object data) throws Exception {
            toProxy.onZkDataChange(zkClient, dataPath, data.toString());
        }

        @Override
        public void handleDataDeleted(String dataPath) throws Exception {
            handleDataChange(dataPath, null);
        }

        boolean unsubscribeIfIsListenerFor(String path, ZKEventListener listener, ZkClient client) {
            // yes, reference equality.
            boolean ret = path.equals(getPath()) && listener == this.toProxy;
            if(ret) {
                client.unsubscribeDataChanges(path, this);
            }

            return ret;
        }
    }
}
