package com.bazaarvoice.zookeeper.test;

import com.bazaarvoice.zookeeper.ZooKeeperConfiguration;
import com.bazaarvoice.zookeeper.ZooKeeperConnection;
import com.bazaarvoice.zookeeper.internal.CuratorConnection;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;
import com.netflix.curator.retry.RetryNTimes;
import com.netflix.curator.test.InstanceSpec;
import com.netflix.curator.test.KillSession;
import com.netflix.curator.test.TestingServer;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class ZooKeeperTest {
    private TestingServer _zooKeeperServer;
    private boolean _started;
    private InstanceSpec _instanceSpec;

    /** All of the curator instances that we've created running the test. */
    private List<CuratorFramework> _curatorInstances = Lists.newArrayList();

    /** All of the connection instances that we've created running the test. */
    private List<ZooKeeperConnection> _connections = Lists.newArrayList();

    @Before
    public void setup() throws Exception {
        _instanceSpec = InstanceSpec.newInstanceSpec();
        startZooKeeper();
    }

    @After
    public void teardown() throws Exception {
        for (ZooKeeperConnection connection : _connections) {
            Closeables.closeQuietly(connection);
        }
        for (CuratorFramework curator : _curatorInstances) {
            Closeables.closeQuietly(curator);
        }

        Closeables.closeQuietly(_zooKeeperServer);
    }

    public void startZooKeeper() throws Exception {
        if (!_started) {
            _zooKeeperServer = new TestingServer(_instanceSpec);
            _started = true;
        }
    }

    public void stopZooKeeper() throws IOException {
        if (_started) {
            _zooKeeperServer.stop();
            _started = false;
        }
    }

    public void restartZooKeeper() throws Exception {
        stopZooKeeper();
        startZooKeeper();
    }

    public ZooKeeperConnection newZooKeeperConnection() throws Exception {
        // For test case purposes don't retry at all.  This should never be done in production!!!
        return newZooKeeperConnection(new ZooKeeperConfiguration()
                .withBoundedExponentialBackoffRetry(100, 1000, 1));
    }

    public ZooKeeperConnection newZooKeeperConnection(ZooKeeperConfiguration configuration) {
        assertNotNull("ZooKeeper testing server is null, did you forget to call super.setup()", _zooKeeperServer);

        ZooKeeperConnection connection = configuration
                .withConnectString(_instanceSpec.getConnectString())
                .connect();

        _connections.add(connection);

        return connection;
    }

    public CuratorFramework newCurator() throws Exception {
        return newCurator(CuratorFrameworkFactory.builder().retryPolicy(new RetryNTimes(0, 0)));
    }

    public CuratorFramework newCurator(CuratorFrameworkFactory.Builder builder) throws Exception {
        assertNotNull("ZooKeeper testing server is null, did you forget to call super.setup()", _zooKeeperServer);

        CuratorFramework curator = builder
                .connectString(_instanceSpec.getConnectString())
                .build();
        curator.start();

        _curatorInstances.add(curator);

        return curator;
    }

    public ZooKeeperConnection newMockZooKeeperConnection(CuratorFramework curator) throws Exception {
        CuratorConnection connection = mock(CuratorConnection.class);
        when(connection.getCurator()).thenReturn(curator);
        return connection;
    }

    public ZooKeeperConnection newMockZooKeeperConnection() throws Exception {
        CuratorFramework curator = mock(CuratorFramework.class);
        when(curator.isStarted()).thenReturn(true);
        return newMockZooKeeperConnection(curator);
    }

    @SuppressWarnings("unused")
    public void killSession(ZooKeeperConnection connection) throws Exception {
        killSession(((CuratorConnection)connection).getCurator());
    }

    public void killSession(CuratorFramework curator) throws Exception {
        KillSession.kill(curator.getZookeeperClient().getZooKeeper(),
                curator.getZookeeperClient().getCurrentConnectionString());
    }

    public static class Trigger {
        private final CountDownLatch _latch;

        public Trigger() {
            _latch = new CountDownLatch(1);
        }

        public void fire() {
            _latch.countDown();
        }

        public boolean firedWithin(long duration, TimeUnit unit) {
            try {
                return _latch.await(duration, unit);
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    public static class WatchTrigger extends Trigger implements Watcher {
        private final Event.EventType _expected;

        public static WatchTrigger creationTrigger() {
            return new WatchTrigger(Event.EventType.NodeCreated);
        }

        public static WatchTrigger deletionTrigger() {
            return new WatchTrigger(Event.EventType.NodeDeleted);
        }

        public WatchTrigger(Event.EventType expected) {
            _expected = expected;
        }

        @Override
        public void process(WatchedEvent event) {
            if (_expected.equals(event.getType())) {
                fire();
            }
        }
    }

    public static class ConnectionTrigger extends Trigger implements ConnectionStateListener {
        private final ConnectionState _expected;

        public static ConnectionTrigger lostTrigger() {
            return new ConnectionTrigger(ConnectionState.LOST);
        }

        public static ConnectionTrigger reconnectedTrigger() {
            return new ConnectionTrigger(ConnectionState.RECONNECTED);
        }

        public ConnectionTrigger(ConnectionState expected) {
            _expected = expected;
        }

        @Override
        public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
            if (_expected.equals(connectionState)) {
                fire();
            }
        }
    }
}
