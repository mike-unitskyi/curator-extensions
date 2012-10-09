package com.bazaarvoice.zookeeper.recipes.discovery;

import com.bazaarvoice.zookeeper.ZooKeeperConfiguration;
import com.bazaarvoice.zookeeper.ZooKeeperConnection;
import com.netflix.curator.test.ByteCodeRewrite;
import com.netflix.curator.test.InstanceSpec;
import com.netflix.curator.test.QuorumConfigBuilder;
import com.netflix.curator.test.TestingZooKeeperServer;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

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
        NodeTrigger lostTrigger = new NodeTrigger();
        nodeDiscovery.addListener(lostTrigger);

        nodeDiscovery.start();

        server.stop();

        assertTrue(lostTrigger.lostWithin(10, TimeUnit.SECONDS));

        // Listen for reconnection.
        NodeTrigger reconnectTrigger = new NodeTrigger();
        nodeDiscovery.addListener(reconnectTrigger);

        server.restart();

        assertTrue(reconnectTrigger.reconnectedWithin(10, TimeUnit.SECONDS));

        nodeDiscovery.close();
        server.close();
    }
}
