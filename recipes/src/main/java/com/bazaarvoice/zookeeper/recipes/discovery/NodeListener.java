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
     * Notification that ZooKeeper's connection was Suspend, Lost, or Reconnected. The semantics of a reset imply that
     * onNodeRemoved() is going to be called for each currently active node immediately following this reset event. If
     * this was due to a Reconnected event then onNodeAdded() will be called for each node already active in ZooKeeper.
     */
    void onZooKeeperReset();
}
