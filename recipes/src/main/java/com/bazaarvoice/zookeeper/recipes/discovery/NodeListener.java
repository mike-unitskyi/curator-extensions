package com.bazaarvoice.zookeeper.recipes.discovery;

/**
 * Listener interface that is notified when nodes are added, removed, or updated.
 *
 * @param <T> The type that {@code ZooKeeperNodeDiscovery} will use to represent an active node.
 */
public interface NodeListener<T> {
    /**
     * Notification that a node was created at {@code path} and its data is represented by {@code node}.
     *
     * @param path ZooKeeper path of the node.
     * @param node Logical representation of the node.
     */
    void onNodeAdded(String path, T node);

    /**
     * Notification that a node was removed from {@code path} and its data was represented by {@code node}.
     *
     * @param path ZooKeeper path of the node.
     * @param node Logical representation of the node.
     */
    void onNodeRemoved(String path, T node);

    /**
     * Notification that a node's data was updated at {@code path} and its data is represented by {@code node}.
     *
     * @param path ZooKeeper path of the node.
     * @param node Logical representation of the node.
     */
    void onNodeUpdated(String path, T node);

    /**
     * Notification that a ZooKeeper connection event happened. Can be suspended, lost, or reconnected. After a
     * reconnect, changes from the previously known state of the world will be reported. The actual sequence of events
     * during the loss of connection is not reported, merely the total delta between connection loss and reconnection.
     * Notable, this means that there is no guarantee that nodes that changed did so in a single transaction, nor that
     * unchanged nodes did not change and then change back during the connection disruption.
     *
     * @param event The ZooKeeper connection state event.
     */
    void onZooKeeperEvent(ZooKeeperEvent event);

    enum ZooKeeperEvent {
        /**
         * Connection suspended. May be reconnected.
         */
        SUSPENDED,
        /**
         * Connection lost. Probably will not be reconnected (but not guaranteed not to be).
         */
        LOST,
        /**
         * Reconnected. Change i
         */
        RECONNECTED,
    }
}
