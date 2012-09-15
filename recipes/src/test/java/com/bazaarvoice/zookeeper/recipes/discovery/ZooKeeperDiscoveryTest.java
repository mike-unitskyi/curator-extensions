package com.bazaarvoice.zookeeper.recipes.discovery;

import com.bazaarvoice.zookeeper.ZooKeeperConnection;
import com.bazaarvoice.zookeeper.recipes.ZooKeeperPersistentEphemeralNode;
import com.bazaarvoice.zookeeper.recipes.discovery.ChildDataParser;
import com.bazaarvoice.zookeeper.recipes.discovery.NodeListener;
import com.bazaarvoice.zookeeper.recipes.discovery.ZooKeeperDiscovery;
import com.bazaarvoice.zookeeper.test.ZooKeeperTest;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.ChildData;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ZooKeeperDiscoveryTest extends ZooKeeperTest {
    private static class Node {
        private final String _name;

        public Node(String name) {
            _name = name;
        }

        public String getName() {
            return _name;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(_name);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof Node)) return false;

            Node that = (Node) obj;
            return Objects.equal(_name, that.getName());
        }

        public static final ChildDataParser<Node> PARSER = new ChildDataParser<Node>() {
            @Override
            public Node parse(ChildData input) {
                return new Node(new String(input.getData(), Charsets.UTF_8));
            }
        };
    }

    private final Map<String, ZooKeeperPersistentEphemeralNode> _nodes = Maps.newConcurrentMap();

    private void register(Node node) throws Exception {
        ZooKeeperPersistentEphemeralNode zkNode = new ZooKeeperPersistentEphemeralNode(
            newZooKeeperConnection(),
            makePath(node),
            node.getName().getBytes(Charsets.UTF_8),
            CreateMode.EPHEMERAL_SEQUENTIAL
        );

        _nodes.put(node.getName(), zkNode);
    }

    private void unregister(Node node) {
        ZooKeeperPersistentEphemeralNode zkNode = _nodes.get(node.getName());
        if (zkNode != null) {
            zkNode.close(10, TimeUnit.SECONDS);
        }
        _nodes.remove(node.getName());
    }

    private static String makeBasePath(Node node) {
        return ZKPaths.makePath(node.getName(), "");
    }

    private static String makePath(Node node) {
        return ZKPaths.makePath(node.getName(), node.getName());
    }

    private static final Node FOO = new Node("Foo");

    private static final Node BAR = new Node("Bar");

    private ZooKeeperDiscovery<Node> _discovery;

    @Override
    public void setup() throws Exception {
        super.setup();
        _discovery = new ZooKeeperDiscovery<Node>(newCurator(), makeBasePath(FOO), Node.PARSER);
    }

    @Override
    public void teardown() throws Exception {
        Closeables.closeQuietly(_discovery);

        for (ZooKeeperPersistentEphemeralNode node : _nodes.values()) {
            node.close(10, TimeUnit.SECONDS);
        }
        _nodes.clear();

        super.teardown();
    }

    @Test (expected = NullPointerException.class)
    public void testNullConfiguration() {
        new ZooKeeperDiscovery<Node>((ZooKeeperConnection) null, makeBasePath(FOO), Node.PARSER);
    }

    @Test (expected = NullPointerException.class)
    public void testNullServiceName() throws Exception {
        new ZooKeeperDiscovery<Node>(newCurator(), null, Node.PARSER);
    }

    @Test (expected = IllegalArgumentException.class)
    public void testEmptyServiceName() throws Exception {
        new ZooKeeperDiscovery<Node>(newCurator(), "", Node.PARSER);
    }

    @Test (expected = NullPointerException.class)
    public void testNullParser() throws Exception {
        new ZooKeeperDiscovery<Node>(newCurator(), makeBasePath(FOO), null);
    }

    @Test
    public void testRegisterService() throws Exception {
        register(FOO);
        assertTrue(waitUntilSize(_discovery.getNodes(), 1));
    }

    @Test
    public void testUnregisterService() throws Exception {
        register(FOO);
        assertTrue(waitUntilSize(_discovery.getNodes(), 1));

        unregister(FOO);
        assertTrue(waitUntilSize(_discovery.getNodes(), 0));
    }

    @Test
    public void testClose() throws Exception {
        // After closing, HostDiscovery returns no hosts so clients won't work if they accidentally keep using it.
        register(FOO);
        assertTrue(waitUntilSize(_discovery.getNodes(), 1));
        _discovery.close();
        assertTrue(Iterables.isEmpty(_discovery.getNodes()));
        _discovery = null;
    }

    @Test
    public void testWaitForData() throws Exception {
        // Create the HostDiscovery after registration is done so there's at least one initial host
        register(FOO);
        ZooKeeperDiscovery<Node> discovery = new ZooKeeperDiscovery<Node>(
            newCurator(),
            makeBasePath(FOO),
            Node.PARSER
        );

        assertEquals(Iterables.size(discovery.getNodes()), 1);
    }

    @Test
    public void testMembershipCheck() throws Exception {
        register(FOO);
        register(BAR);
        assertTrue(waitUntilSize(_discovery.getNodes(), 1));
        assertTrue(_discovery.contains(FOO));
        assertFalse(_discovery.contains(BAR));
    }

    @Test
    public void testAlreadyExistingEndPointsDoNotFireEvents() throws Exception {
        register(FOO);

        ZooKeeperDiscovery<Node> discovery = new ZooKeeperDiscovery<Node>(
            newCurator(),
            makeBasePath(FOO),
            Node.PARSER
        );

        assertEquals(Iterables.size(discovery.getNodes()), 1);

        CountingListener eventCounter = new CountingListener();
        discovery.addListener(eventCounter);

        // Don't know when the register() will take effect.  Execute and wait for an
        // unregister--that should be long enough to wait.
        unregister(FOO);
        assertTrue(waitUntilSize(discovery.getNodes(), 0));

        assertEquals(0, eventCounter.getNumAdds());  // endPoints initially visible never fire add events
    }

    @Test
    public void testServiceRemovedWhenSessionKilled() throws Exception {
        register(FOO);
        assertTrue(waitUntilSize(_discovery.getNodes(), 1));

        killSession(_discovery.getCurator());

        // The entry gets cleaned up because we've lost contact with ZooKeeper
        assertTrue(waitUntilSize(_discovery.getNodes(), 0));
    }

    @Test
    public void testServiceReRegisteredWhenSessionKilled() throws Exception {
        register(FOO);
        assertTrue(waitUntilSize(_discovery.getNodes(), 1));

        killSession(_discovery.getCurator());

        // The entry gets cleaned up because we've lost contact with ZooKeeper
        assertTrue(waitUntilSize(_discovery.getNodes(), 0));

        // Then it automatically gets created when the connection is re-established with ZooKeeper
        assertTrue(waitUntilSize(_discovery.getNodes(), 1));
    }

    @Test
    public void testRegisterServiceCallsListener() throws Exception {
        NodeTrigger trigger = new NodeTrigger();
        _discovery.addListener(trigger);

        register(FOO);
        assertTrue(trigger.addedWithin(10, TimeUnit.SECONDS));
    }

    @Test
    public void testUnregisterServiceCallsListener() throws Exception {
        NodeTrigger trigger = new NodeTrigger();
        _discovery.addListener(trigger);

        register(FOO);
        assertTrue(trigger.addedWithin(10, TimeUnit.SECONDS));

        unregister(FOO);
        assertTrue(trigger.removedWithin(10, TimeUnit.SECONDS));
    }

    @Test
    public void testRemovedListenerDoesNotSeeEvents() throws Exception {
        NodeTrigger trigger = new NodeTrigger();
        _discovery.addListener(trigger);

        CountingListener eventCounter = new CountingListener();
        _discovery.addListener(eventCounter);
        _discovery.removeListener(eventCounter);

        register(FOO);
        assertTrue(trigger.addedWithin(10, TimeUnit.SECONDS));

        unregister(FOO);
        assertTrue(trigger.removedWithin(10, TimeUnit.SECONDS));

        assertEquals(0, eventCounter.getNumEvents());
    }

    @Test
    public void testListenerCalledWhenSessionKilled() throws Exception {
        NodeTrigger trigger = new NodeTrigger();
        _discovery.addListener(trigger);

        register(FOO);
        assertTrue(trigger.addedWithin(10, TimeUnit.SECONDS));

        killSession(_discovery.getCurator());

        // The entry gets cleaned up because we've lost contact with ZooKeeper
        assertTrue(trigger.removedWithin(10, TimeUnit.SECONDS));
    }

    @Test
    public void testListenerCalledWhenServiceIsReregisteredAfterSessionKilled() throws Exception {
        NodeTrigger initialTrigger = new NodeTrigger();
        _discovery.addListener(initialTrigger);

        register(FOO);
        assertTrue(initialTrigger.addedWithin(10, TimeUnit.SECONDS));

        NodeTrigger trigger = new NodeTrigger();
        _discovery.addListener(trigger);

        killSession(_discovery.getCurator());

        // The entry gets cleaned up because we've lost contact with ZooKeeper
        assertTrue(trigger.removedWithin(10, TimeUnit.SECONDS));

        // Then it automatically gets created when the connection is re-established with ZooKeeper
        assertTrue(trigger.addedWithin(10, TimeUnit.SECONDS));
    }

    @Test
    public void testMultipleListeners() throws Exception {
        NodeTrigger trigger1 = new NodeTrigger();
        NodeTrigger trigger2 = new NodeTrigger();
        _discovery.addListener(trigger1);
        _discovery.addListener(trigger2);

        register(FOO);
        assertTrue(trigger1.addedWithin(10, TimeUnit.SECONDS));
        assertTrue(trigger2.addedWithin(10, TimeUnit.SECONDS));

        unregister(FOO);
        assertTrue(trigger1.removedWithin(10, TimeUnit.SECONDS));
        assertTrue(trigger2.removedWithin(10, TimeUnit.SECONDS));
    }

    @Test
    public void testInitializeRacesRemove() throws Exception {
        // Create a new ZK connection now so it's ready-to-go when we need it.
        CuratorFramework curator = newCurator();

        // Register FOO and wait until it's visible.
        register(FOO);
        assertTrue(waitUntilSize(_discovery.getNodes(), 1));

        // Unregister FOO and create a new HostDiscovery instance as close together as we can, so they race.
        unregister(FOO);
        ZooKeeperDiscovery<Node> discovery = new ZooKeeperDiscovery<Node>(
            curator,
            makeBasePath(FOO),
            Node.PARSER
        );

        assertTrue(waitUntilSize(discovery.getNodes(), 0));
    }

    private static <T> boolean waitUntilSize(Iterable<T> iterable, int size, long timeout, TimeUnit unit) {
        long start = System.nanoTime();
        while (System.nanoTime() - start <= unit.toNanos(timeout)) {
            if (Iterables.size(iterable) == size) {
                return true;
            }

            Thread.yield();
        }

        return false;
    }

    private static <T> boolean waitUntilSize(Iterable<T> iterable, int size) {
        return waitUntilSize(iterable, size, 10, TimeUnit.SECONDS);
    }

    private static final class NodeTrigger implements NodeListener<Node> {
        private final Trigger _addTrigger = new Trigger();
        private final Trigger _removeTrigger = new Trigger();

        @Override
        public void onNodeAdded(Node node) {
            _addTrigger.fire();
        }

        @Override
        public void onNodeRemoved(Node node) {
            _removeTrigger.fire();
        }

        public boolean addedWithin(long duration, TimeUnit unit) throws InterruptedException {
            return _addTrigger.firedWithin(duration, unit);
        }

        public boolean removedWithin(long duration, TimeUnit unit) throws InterruptedException {
            return _removeTrigger.firedWithin(duration, unit);
        }
    }

    private static final class CountingListener implements NodeListener<Node> {
        private int _numAdds;
        private int _numRemoves;

        @Override
        public void onNodeAdded(Node node) {
            _numAdds++;
        }

        @Override
        public void onNodeRemoved(Node node) {
            _numRemoves++;
        }

        public int getNumAdds() {
            return _numAdds;
        }

        public int getNumEvents() {
            return _numAdds + _numRemoves;
        }
    }
}
