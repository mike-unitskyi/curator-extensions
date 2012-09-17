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

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ZooKeeperNodeDiscovery<T> implements Closeable {
    private final CuratorFramework _curator;
    private final Map<String, T> _nodes;
    private final Set<NodeListener<T>> _listeners;
    private final PathChildrenCache _pathCache;
    private final NodeDataParser<T> _nodeDataParser;

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
                    addNode(childData.getPath(), _nodeDataParser.parse(childData.getData()));
                }
            }
        } catch (Throwable t) {
            Closeables.closeQuietly(this);
            throw Throwables.propagate(t);
        }
    }

    public Iterable<T> getNodes() {
        return Iterables.unmodifiableIterable(_nodes.values());
    }

    public boolean contains(T node) {
        return _nodes.containsValue(node);
    }

    public void addListener(NodeListener<T> listener) {
        _listeners.add(listener);
    }

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
            T nodeData = _nodeDataParser.parse(event.getData().getData());
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
