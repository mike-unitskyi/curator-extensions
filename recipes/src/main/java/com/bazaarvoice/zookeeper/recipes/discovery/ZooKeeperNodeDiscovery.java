package com.bazaarvoice.zookeeper.recipes.discovery;

import com.bazaarvoice.zookeeper.ZooKeeperConnection;
import com.bazaarvoice.zookeeper.internal.CuratorConnection;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The {@code ZooKeeperNodeDiscovery} class is used to watch a path in ZooKeeper. It will monitor which nodes
 * exist and fire node change events to subscribed instances of {@code NodeListener}. Users of this class should not
 * cache the results of discovery as subclasses can choose to change the set of available nodes based on some external
 * mechanism (ex. using bouncer).
 *
 * @param <T> The type that will be used to represent an active node.
 */
public class ZooKeeperNodeDiscovery<T> implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperNodeDiscovery.class);

    /** How long in milliseconds to wait between attempts to start. */
    private static final long WAIT_DURATION_IN_MILLIS = 100;

    private final CuratorFramework _curator;
    private final Map<String, Optional<T>> _nodes;
    private final Set<NodeListener<T>> _listeners;
    private final PathChildrenCache _pathCache;
    private final NodeDataParser<T> _nodeDataParser;
    private final ScheduledExecutorService _executor;

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
        _pathCache = new PathChildrenCache(_curator, nodePath, true, threadFactory);
        _nodeDataParser = parser;
        _executor = Executors.newSingleThreadScheduledExecutor(threadFactory);
    }

    /**
     * Start the NodeDiscovery.
     */
    public void start() {
        _pathCache.getListenable().addListener(new PathListener());
        startPathCache();
    }

    /**
     * Start the underlying path cache and then populate the data for any nodes that existed prior to being created and
     * connected to ZooKeeper.
     * <p/>
     * This must be synchronized so async remove events aren't processed between start() and adding nodes.
     * Use synchronous start(true) instead of asynchronous start(false) so we can tell when it's done and the
     * node discovery set is usable.
     * <p/>
     * If there is a problem starting the path cache then we'll continue attempting to start it in a background thread.
     */
    private synchronized void startPathCache() {
        try {
            _pathCache.start(true);
        } catch (Throwable t) {
            waitThenStartPathCache();
            return;
        }

        for (ChildData childData : _pathCache.getCurrentData()) {
            addNode(childData.getPath(), parseChildData(childData));
        }
    }

    /**
     * Wait a short period of time then try to start the path cache.
     */
    private void waitThenStartPathCache() {
        _executor.schedule(new Runnable() {
            @Override
            public void run() {
                startPathCache();
            }
        }, WAIT_DURATION_IN_MILLIS, TimeUnit.MILLISECONDS);
    }

    /**
     * Retrieve the available nodes.
     *
     * @return The available nodes.
     */
    public Map<String, T> getNodes() {
        return Maps.transformValues(Collections.unmodifiableMap(_nodes), new Function<Optional<T>, T>() {
            @Override
            public T apply(Optional<T> input) {
                return input.orNull();
            }
        });
    }

    /**
     * Returns true if the specified node is a member of the iterable returned by {@link #getNodes()}.
     *
     * @param node The node to test.
     * @return True if the specified node is a member of the iterable returned by {@link #getNodes()}.
     */
    public boolean contains(T node) {
        return _nodes.containsValue(Optional.fromNullable(node));
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
        if (_nodes.put(path, Optional.fromNullable(node)) == null) {
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
        Optional<T> oldNode = _nodes.put(path, Optional.fromNullable(node));
        if (!Objects.equal(oldNode.orNull(), node)) {
            fireUpdateEvent(path, node);
        }
    }

    private synchronized void clearNodes() {
        // synchronize the modification of _nodes and firing of events so listeners always receive events in the
        // order they occur.
        Map<String, Optional<T>> nodes = ImmutableMap.copyOf(_nodes);
        _nodes.clear();

        fireResetEvent();

        for (String path : nodes.keySet()) {
            fireRemoveEvent(path, nodes.get(path).orNull());
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

    private void fireResetEvent() {
        for (NodeListener<T> listener : _listeners) {
            listener.onZooKeeperReset();
        }
    }

    private T parseChildData(ChildData childData) {
        T value = null;
        try {
            value = _nodeDataParser.parse(childData.getPath(), childData.getData());
        } catch (Exception e) {
            LOG.warn("NodeDataParser failed to parse ZooKeeper data. ZooKeeperPath: {}; Exception Message: {}",
                    childData.getPath(), e.getMessage());
            LOG.warn("Exception", e);
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
