package com.bazaarvoice.zookeeper.recipes.discovery;

import com.bazaarvoice.zookeeper.ZooKeeperConnection;
import com.bazaarvoice.zookeeper.internal.CuratorConnection;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
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
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ThreadFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ZooKeeperDiscovery<T> implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperDiscovery.class);

    private final CuratorFramework _curator;
    private final Set<T> _nodes;
    private final Set<NodeListener<T>> _listeners;
    private final PathChildrenCache _pathCache;
    private final ChildDataParser<T> _childDataParser;

    public ZooKeeperDiscovery(ZooKeeperConnection connection, String nodePath, ChildDataParser<T> parser) {
        this(((CuratorConnection) checkNotNull(connection)).getCurator(), nodePath, parser);
    }

    @VisibleForTesting
    ZooKeeperDiscovery(CuratorFramework curator, String nodePath, ChildDataParser<T> parser) {
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
        _nodes = Sets.newSetFromMap(Maps.<T, Boolean>newConcurrentMap());
        _listeners = Sets.newSetFromMap(Maps.<NodeListener<T>, Boolean>newConcurrentMap());
        _childDataParser = parser;

        _pathCache = new PathChildrenCache(_curator, nodePath, true, threadFactory);
        try {
            _pathCache.getListenable().addListener(new ZkNodeListener());

            // This must be synchronized so async remove events aren't processed between start() and adding end points.
            // Use synchronous start(true) instead of asynchronous start(false) so we can tell when it's done and the
            // HostDiscovery set is usable.
            synchronized (this) {
                _pathCache.start(true);
                for (ChildData childData : _pathCache.getCurrentData()) {
                    addNode(_childDataParser.parse(childData));
                }
            }
        } catch (Throwable t) {
            Closeables.closeQuietly(this);
            throw Throwables.propagate(t);
        }
    }

    public Iterable<T> getNodes() {
        return Iterables.unmodifiableIterable(_nodes);
    }

    public boolean contains(T node) {
        return _nodes.contains(node);
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

    private synchronized void addNode(T node) {
        // synchronize the modification of _nodes and firing of events so listeners always receive events in the
        // order they occur.
        if (_nodes.add(node)) {
            fireAddEvent(node);
        }
    }

    private synchronized void removeNode(T node) {
        // synchronize the modification of _nodes and firing of events so listeners always receive events in the
        // order they occur.
        if (_nodes.remove(node)) {
            fireRemoveEvent(node);
        }
    }

    private synchronized void clearEndPoints() {
        // synchronize the modification of _nodes and firing of events so listeners always receive events in the
        // order they occur.
        Collection<T> nodes = ImmutableList.copyOf(_nodes);
        _nodes.clear();
        for (T node : nodes) {
            fireRemoveEvent(node);
        }
    }

    private void fireAddEvent(T node) {
        for (NodeListener<T> listener : _listeners) {
            listener.onNodeAdded(node);
        }
    }

    private void fireRemoveEvent(T node) {
        for (NodeListener<T> listener : _listeners) {
            listener.onNodeRemoved(node);
        }
    }

    /**
     * A curator <code>PathChildrenCacheListener</code>
     */
    private final class ZkNodeListener implements PathChildrenCacheListener {
        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
            if (event.getType() == PathChildrenCacheEvent.Type.RESET) {
                clearEndPoints();
                return;
            }

            T endPoint = _childDataParser.parse(event.getData());
            switch (event.getType()) {
                case CHILD_ADDED:
                    addNode(endPoint);
                    break;

                case CHILD_REMOVED:
                    removeNode(endPoint);
                    break;

                case CHILD_UPDATED:
                    LOG.info("Node data changed unexpectedly. ZooKeeperPath {}", event.getData().getPath());
                    break;
            }
        }
    }
}
