package com.bazaarvoice.zookeeper.recipes.discovery;

/**
 * The {@code NodeDataParser} class is used to encapsulate the strategy that converts ZooKeeper node data into
 * a logical format for the user of {@code NodeDataParser}.
 *
 * @param <T> The type that {@code ZooKeeperNodeDiscovery} will use to represent an active node.
 **/
public interface NodeDataParser<T> {
    /**
     * Converts ZooKeeper's internal byte representation into a custom representation of {@code <T>}.
     * If an exception is thrown inside this method, it is treated as if null was returned.
     *
     * @param nodeData A {@code byte[]} representation of ZooKeeper node data.
     * @return An instance of {@code T} that represents {@code nodeData}.
     */
    T parse(byte[] nodeData);
}
