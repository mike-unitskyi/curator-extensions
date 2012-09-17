package com.bazaarvoice.zookeeper.recipes.discovery;

import com.bazaarvoice.zookeeper.ZooKeeperConnection;
import com.bazaarvoice.zookeeper.internal.CuratorConnection;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.ChildData;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The {@code ZooKeeperNodeDiscovery} class is used to watch a path in ZooKeeper. It will monitor which nodes
 * exist and fire node change events to subscribed instances of {@code NodeListener}. Users of
 * this class should not cache the results of discovery as subclasses can choose to change the set of available hosts
 * based on some external mechanism (ex. using bouncer).
 *
 * @param <T> The type that will be used to represent an active node.
 */
public class ZooKeeperNodeDiscovery<T> implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperNodeDiscovery.class);

    private final CuratorFramework _curator;
    private final Map<String, T> _nodes;
    private final Set<NodeListener<T>> _listeners;
    private final PathChildrenCache _pathCache;
    private final NodeDataParser<T> _nodeDataParser;

    /**
     * Creates an instance of {@code ZooKeeperNodeDiscovery}.
     *
     * @param connection ZooKeeper connection.
     * @param nodePath   The path in ZooKeeper to watch.
     * @param parser     The strategy to convert from ZooKeeper {@code byte[]} to {@code T}.
     */
    public ZooKeeperNodeDiscovery(ZooKeeperConnection connection, String nodePath, NodeDataParser<T> parser) {
        this(((CuratorConnection) checkNotNull(connection)).getCurator(), nodePath, parser);
    }

    @VisibleForTesting
    ZooKeeperNodeDiscovery(CuratorFramework curator, String nodePath, NodeDataParser<T> parser) {
        checkNotNull(curator);
        checkNotNull(nodePath);
        checkNotNull(parser);
        checkArgument(curator.isStarted());
        checkArgument(!"".equals(nodePath));

        ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setNameFormat(getClass().getSimpleName() + "(" + nodePath + ")-%d")
            .setDaemon(true)
            .build();

        _curator = curator;
        _nodes = Maps.newConcurrentMap();
        _listeners = Sets.newSetFromMap(Maps.<NodeListener<T>, Boolean>newConcurrentMap());
        _nodeDataParser = parser;

        _pathCache = new PathChildrenCache(_curator, nodePath, true, threadFactory);
        try {
            _pathCache.getListenable().addListener(new PathListener());

            // This must be synchronized so async remove events aren't processed between start() and adding nodes.
            // Use synchronous start(true) instead of asynchronous start(false) so we can tell when it's done and the
            // HostDiscovery set is usable.
            synchronized (this) {
                _pathCache.start(true);
                for (ChildData childData : _pathCache.getCurrentData()) {
                    addNode(childData.getPath(), parseChildData(childData));
                }
            }
        } catch (Throwable t) {
            Closeables.closeQuietly(this);
            throw Throwables.propagate(t);
        }
    }

    /**
     * Retrieve the available nodes.
     *
     * @return The available nodes.
     */
    public Iterable<T> getNodes() {
        return Iterables.unmodifiableIterable(_nodes.values());
    }

    /**
     * Returns true if the specified node is a member of the iterable returned by {@link #getNodes()}.
     *
     * @param node The node to test.
     * @return True if the specified node is a member of the iterable returned by {@link #getNodes()}.
     */
    public boolean contains(T node) {
        return _nodes.containsValue(node);
    }

    /**
     * Add a node listener.
     *
     * @param listener The node listener to add.
     */
    public void addListener(NodeListener<T> listener) {
        _listeners.add(listener);
    }

    /**
     * Remove a node listener.
     *
     * @param listener The node listener to remove.
     */
    public void removeListener(NodeListener<T> listener) {
        _listeners.remove(listener);
    }

    @Override
    public synchronized void close() throws IOException {
        _listeners.clear();
        _pathCache.close();
        _nodes.clear();
    }

    @VisibleForTesting
    CuratorFramework getCurator() {
        return _curator;
    }

    private synchronized void addNode(String path, T node) {
        // synchronize the modification of _nodes and firing of events so listeners always receive events in the
        // order they occur.
        if (_nodes.put(path, node) == null) {
            fireAddEvent(path, node);
        }
    }

    private synchronized void removeNode(String path, T node) {
        // synchronize the modification of _nodes and firing of events so listeners always receive events in the
        // order they occur.
        if (_nodes.remove(path) != null) {
            fireRemoveEvent(path, node);
        }
    }

    private synchronized void updateNode(String path, T node) {
        // synchronize the modification of _nodes and firing of events so listeners always receive events in the
        // order they occur.
        T oldNode = _nodes.put(path, node);
        if (oldNode == null || !oldNode.equals(node)) {
            fireUpdateEvent(path, node);
        }
    }

    private synchronized void clearNodes() {
        // synchronize the modification of _nodes and firing of events so listeners always receive events in the
        // order they occur.
        Map<String, T> nodes = ImmutableMap.copyOf(_nodes);
        _nodes.clear();
        for (String path : nodes.keySet()) {
            fireRemoveEvent(path, nodes.get(path));
        }
    }

    private void fireAddEvent(String path, T node) {
        for (NodeListener<T> listener : _listeners) {
            listener.onNodeAdded(path, node);
        }
    }

    private void fireRemoveEvent(String path, T node) {
        for (NodeListener<T> listener : _listeners) {
            listener.onNodeRemoved(path, node);
        }
    }

    private void fireUpdateEvent(String path, T node) {
        for (NodeListener<T> listener : _listeners) {
            listener.onNodeUpdated(path, node);
        }
    }

    private T parseChildData(ChildData childData) {
        T value = null;
        try {
            value = _nodeDataParser.parse(childData.getData());
        } catch (Exception e) {
            LOG.info("NodeDataParser failed to parse ZooKeeper data. ZooKeeperPath: {}; Exception Message: {}",
                childData.getPath(), e.getMessage());
        }

        return value;
    }

    /**
     * A curator <code>PathChildrenCacheListener</code>
     */
    private final class PathListener implements PathChildrenCacheListener {
        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
            if (event.getType() == PathChildrenCacheEvent.Type.RESET) {
                clearNodes();
                return;
            }

            String nodePath = event.getData().getPath();
            T nodeData = parseChildData(event.getData());
            switch (event.getType()) {
                case CHILD_ADDED:
                    addNode(nodePath, nodeData);
                    break;

                case CHILD_REMOVED:
                    removeNode(nodePath, nodeData);
                    break;

                case CHILD_UPDATED:
                    updateNode(nodePath, nodeData);
                    break;
            }
        }
    }
}
