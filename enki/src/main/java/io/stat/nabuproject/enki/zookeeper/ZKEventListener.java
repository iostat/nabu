package io.stat.nabuproject.enki.zookeeper;

import java.util.List;

/**
 * An interface that describes something which can register
 * against a {@link ZKEventSource} to be notified of changes
 * in the ZK store.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface ZKEventListener {
    /**
     * Called by a {@link ZKEventSource} when a node that was specified by a subscriber
     * to be listened to for changes has changes in the set of its children
     * @param sender the ZKClient that observed the change
     * @param nodeRoot the path that was subscribed to changes
     * @param children the new list of child elements under nodeRoot
     * @return true if whatever the callback was going to do succeeded, false otherwise.
     */
    default boolean onZKPathChange(ZKClient sender, String nodeRoot, List<String> children) { return true; }

    /**
     * Called by a {@link ZKEventSource} when a node that was specified by a subscriber
     * to be listened to for changes in the data has changes
     * @param sender the ZKClient that observed the change
     * @param nodePath the path of the node that had the changes
     * @param newData the new contents of nodePath
     * @return true if whatever the callback was going to do succeeded, false otherwise.
     */
    default boolean onZkDataChange(ZKClient sender, String nodePath, String newData) { return true; }
}
