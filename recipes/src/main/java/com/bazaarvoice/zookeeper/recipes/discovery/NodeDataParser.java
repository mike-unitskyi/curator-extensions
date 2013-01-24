package com.bazaarvoice.zookeeper.recipes.discovery;

/**
 * The {@code NodeDataParser} class is used to encapsulate the strategy that converts ZooKeeper node data into
 * a logical format for the user of {@code NodeDataParser}.
 *
 * @param <T> The type that {@code ZooKeeperNodeDiscovery} will use to represent an active node.
 */
public interface NodeDataParser<T> {
    /**
     * Converts ZooKeeper's internal byte representation into a custom representation of {@code <T>}.
     * If an exception is thrown inside this method, it is treated as if null was returned.
     *
     * It is extremely important that if T is intended to be passed around without the path, that the path be included
     * as part of T to distinguish two nodes which may otherwise appear identical.
     *
     * @param path     This is the zookeeper path that this node data lives on.
     * @param nodeData A {@code byte[]} representation of ZooKeeper node data.
     * @return An instance of {@code T} that represents {@code nodeData}.
     */
    T parse(String path, byte[] nodeData);
}
