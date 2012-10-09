package com.bazaarvoice.zookeeper.recipes.discovery;

import com.bazaarvoice.zookeeper.ZooKeeperConnection;
import com.bazaarvoice.zookeeper.internal.CuratorConnection;
import com.bazaarvoice.zookeeper.test.ZooKeeperTest;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.state.ConnectionState;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class ZooKeeperNodeDiscoveryConnectionTest extends ZooKeeperTest {
    public void setup() {
        // Purposefully don't call super.
    }

    @Test
    public void testWithoutZooKeeper() throws Exception {
        ZooKeeperNodeDiscovery<Node> nodeDiscovery = new ZooKeeperNodeDiscovery<Node>(newZooKeeperConnection(),
                "/foo", Node.PARSER);

        nodeDiscovery.start();

        nodeDiscovery.close();
    }

    @Test
    public void testZooKeeperRestart() throws Exception {
        startZooKeeper();

        ZooKeeperConnection connection = newZooKeeperConnection();
        ZooKeeperNodeDiscovery<Node> nodeDiscovery = new ZooKeeperNodeDiscovery<Node>(connection, "/foo", Node.PARSER);

        CuratorFramework curator = ((CuratorConnection) connection).getCurator();

        ConnectionTrigger lostTrigger = ConnectionTrigger.lostTrigger();
        curator.getConnectionStateListenable().addListener(lostTrigger);

        nodeDiscovery.start();

        stopZooKeeper();

        assertTrue(lostTrigger.firedWithin(10, TimeUnit.SECONDS));

        ConnectionTrigger reconnectedTrigger = new ConnectionTrigger(ConnectionState.RECONNECTED);
        curator.getConnectionStateListenable().addListener(reconnectedTrigger);

        startZooKeeper();

        assertTrue(reconnectedTrigger.firedWithin(10, TimeUnit.SECONDS));

        nodeDiscovery.close();
    }
}
