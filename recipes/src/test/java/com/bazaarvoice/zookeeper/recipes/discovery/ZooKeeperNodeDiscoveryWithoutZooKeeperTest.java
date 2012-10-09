package com.bazaarvoice.zookeeper.recipes.discovery;

import com.bazaarvoice.zookeeper.ZooKeeperConfiguration;
import com.bazaarvoice.zookeeper.ZooKeeperConnection;
import com.bazaarvoice.zookeeper.internal.CuratorConnection;
import com.google.common.collect.Lists;
import com.netflix.curator.CuratorZookeeperClient;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.GetChildrenBuilder;
import com.netflix.curator.framework.listen.Listenable;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;
import com.netflix.curator.test.ByteCodeRewrite;
import com.netflix.curator.test.InstanceSpec;
import com.netflix.curator.test.QuorumConfigBuilder;
import com.netflix.curator.test.TestingZooKeeperServer;
import com.netflix.curator.utils.EnsurePath;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ZooKeeperNodeDiscoveryWithoutZooKeeperTest {
    @Test
    public void testWithoutZooKeeper() {
        // Generate a connect string for an unbound port.
        String connectString = InstanceSpec.newInstanceSpec().getConnectString();

        ZooKeeperConnection connection = new ZooKeeperConfiguration()
                .withConnectString(connectString)
                .withBoundedExponentialBackoffRetry(10, 1000, 1)
                .connect();

        ZooKeeperNodeDiscovery<Node> nodeDiscovery = new ZooKeeperNodeDiscovery<Node>(connection, "/foo", Node.PARSER);

        nodeDiscovery.start();
    }

    @Test
    public void testZooKeeperRestart() throws Exception {
        // This is normally done when starting a TestServer. TestServer doesn't expose restart, so we do this manually.
        ByteCodeRewrite.apply();

        // Get our connect string.
        InstanceSpec spec = InstanceSpec.newInstanceSpec();
        String connectString = spec.getConnectString();

        // Start the server.
        TestingZooKeeperServer server = new TestingZooKeeperServer(new QuorumConfigBuilder(spec));
        server.start();

        ZooKeeperConnection connection = new ZooKeeperConfiguration()
                .withConnectString(connectString)
                .withBoundedExponentialBackoffRetry(10, 1000, 1)
                .connect();

        ZooKeeperNodeDiscovery<Node> nodeDiscovery = new ZooKeeperNodeDiscovery<Node>(connection, "/foo", Node.PARSER);

        // Listen for connection loss.
        Node.NodeTrigger lostTrigger = new Node.NodeTrigger();
        nodeDiscovery.addListener(lostTrigger);

        nodeDiscovery.start();

        server.stop();

        assertTrue(lostTrigger.lostWithin(10, TimeUnit.SECONDS));

        // Listen for reconnection.
        Node.NodeTrigger reconnectTrigger = new Node.NodeTrigger();
        nodeDiscovery.addListener(reconnectTrigger);

        server.restart();

        assertTrue(reconnectTrigger.reconnectedWithin(10, TimeUnit.SECONDS));

        nodeDiscovery.close();
        server.close();
    }
}
