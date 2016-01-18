package io.stat.nabuproject.enki.zookeeper;

import java.util.List;

/**
 * An interface that describes an object that consumers of
 * ZooKeeper events can subscribe against to receive notifications
 * of events such as node or path changes.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface ZKEventSource {
    /**
     * Gets the data under <tt>path</tt>, and subscribes to future changes to the data in
     * <tt>path</tt>. This means that the {@link ZKEventListener#onZkDataChange(ZKClient, String, String)}
     * callback will be called.
     *
     * @param path       the path to get and subscribe to changes for
     * @param subscriber the subscriber to notify of future changes
     * @return the contents of path as the subscription was made
     */
    String getAndSubscribe(String path, ZKEventListener subscriber);

    /**
     * Gets the list of children of <tt>path</tt>, and subscribes to future changes to the data in
     * <tt>path</tt>. This means that the {@link ZKEventListener#onZkDataChange(ZKClient, String, String)}
     * callback will be called.
     *
     * @param path       the path to get and subscribe to changes for
     * @param subscriber the subscriber to notify
     * @param path       the path to subscribe to
     * @param subscriber the subscriber to notify of future changes
     * @return the children of path at the time the subscription began.
     */
    List<String> getChildrenAndSubscribe(String path, ZKEventListener subscriber);

    /**
     * Stops <tt>listener</tt> from being notified when the set of children under
     * <tt>path</tt> changes, if it was. Otherwise does nothing.
     *
     * @param path     the path to stop notifying listener of changes to
     * @param listener the listener who should not longer be notified
     */
    void unsubscribeFromChildEvents(String path, ZKEventListener listener);

    /**
     * Stops <tt>listener</tt> from being notified of changes to the data
     * stored in <tt>path</tt>.
     *
     * @param path     the path to stop notifying listener of changes to
     * @param listener the listener who should no longer be notified
     */
    void unsubscribeFromDataEvents(String path, ZKEventListener listener);

    /**
     * Removes the leader from event notifications for ANY kind of subscribed event
     * @param listener the listener to stop notifying
     */
    void unsubscribeFromAllEvents(ZKEventListener listener);
}
