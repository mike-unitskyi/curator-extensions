package com.bazaarvoice.zookeeper.recipes.discovery;

import com.bazaarvoice.zookeeper.ZooKeeperConnection;
import com.bazaarvoice.zookeeper.recipes.ZooKeeperPersistentEphemeralNode;
import com.bazaarvoice.zookeeper.test.ZooKeeperTest;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ZooKeeperNodeDiscoveryTest extends ZooKeeperTest {

    private final Map<String, ZooKeeperPersistentEphemeralNode> _nodes = Maps.newConcurrentMap();

    private void register(String path, Node node) throws Exception {
        ZooKeeperPersistentEphemeralNode zkNode = new ZooKeeperPersistentEphemeralNode(
                newZooKeeperConnection(),
                makePath(path, node),
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

    private static String makeBasePath(String path) {
        return ZKPaths.makePath(path, "");
    }

    private static String makePath(String path, Node node) {
        return ZKPaths.makePath(path, node.getName());
    }

    private static final String FOO_BUCKET = "foo";
    private static final Node FOO = new Node("Foo");

    private static final String BAR_BUCKET = "bar";
    private static final Node BAR = new Node("Bar");

    private ZooKeeperNodeDiscovery<Node> _nodeDiscovery;

    @Override
    public void setup() throws Exception {
        super.setup();
        _nodeDiscovery = new ZooKeeperNodeDiscovery<Node>(newZooKeeperConnection(), makeBasePath(FOO_BUCKET),
                Node.PARSER);
        _nodeDiscovery.start();
    }

    @Override
    public void teardown() throws Exception {
        Closeables.closeQuietly(_nodeDiscovery);

        for (ZooKeeperPersistentEphemeralNode node : _nodes.values()) {
            node.close(10, TimeUnit.SECONDS);
        }
        _nodes.clear();

        super.teardown();
    }

    @Test(expected = NullPointerException.class)
    public void testNullConnection() {
        new ZooKeeperNodeDiscovery<Node>((ZooKeeperConnection) null, makeBasePath(FOO_BUCKET), Node.PARSER);
    }

    @Test(expected = NullPointerException.class)
    public void testNullPath() throws Exception {
        new ZooKeeperNodeDiscovery<Node>(newZooKeeperConnection(), null, Node.PARSER);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyPath() throws Exception {
        new ZooKeeperNodeDiscovery<Node>(newZooKeeperConnection(), "", Node.PARSER);
    }

    @Test(expected = NullPointerException.class)
    public void testNullParser() throws Exception {
        new ZooKeeperNodeDiscovery<Node>(newZooKeeperConnection(), makeBasePath(FOO_BUCKET), null);
    }

    @Test
    public void testRegisterNode() throws Exception {
        register(FOO_BUCKET, FOO);
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));
    }

    @Test
    public void testUnregisterNode() throws Exception {
        register(FOO_BUCKET, FOO);
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));

        unregister(FOO);
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 0));
    }

    @Test
    public void testClose() throws Exception {
        // After closing, NodeDiscovery returns no nodes so clients won't work if they accidentally keep using it.
        register(FOO_BUCKET, FOO);
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));
        _nodeDiscovery.close();
        assertTrue(_nodeDiscovery.getNodes().isEmpty());
        _nodeDiscovery = null;
    }

    @Test
    public void testWaitForData() throws Exception {
        // Create the NodeDiscovery after registration is done so there's at least one initial node
        register(FOO_BUCKET, FOO);
        ZooKeeperNodeDiscovery<Node> nodeDiscovery = new ZooKeeperNodeDiscovery<Node>(
                newZooKeeperConnection(),
                makeBasePath(FOO_BUCKET),
                Node.PARSER
        );
        nodeDiscovery.start();

        assertEquals(1, nodeDiscovery.getNodes().size());
    }

    @Test
    public void testMembershipCheck() throws Exception {
        register(FOO_BUCKET, FOO);
        register(BAR_BUCKET, BAR);
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));
        assertTrue(_nodeDiscovery.contains(FOO));
        assertFalse(_nodeDiscovery.contains(BAR));
    }

    @Test
    public void testUpdateNode() throws Exception {
        String nodePath = ZKPaths.makePath(FOO_BUCKET, "UpdatingNode");
        CuratorFramework curator = newCurator();

        curator.create().creatingParentsIfNeeded().forPath(nodePath, FOO.getName().getBytes(Charsets.UTF_8));
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));

        NodeTrigger trigger = new NodeTrigger();
        _nodeDiscovery.addListener(trigger);

        curator.setData().forPath(nodePath, BAR.getName().getBytes(Charsets.UTF_8));
        assertTrue(trigger.updatedWithin(10, TimeUnit.SECONDS));
    }

    @Test
    public void testUpdateSizeUnchanged() throws Exception {
        String nodePath = ZKPaths.makePath(FOO_BUCKET, "UpdatingNode");
        CuratorFramework curator = newCurator();

        curator.create().creatingParentsIfNeeded().forPath(nodePath, FOO.getName().getBytes(Charsets.UTF_8));
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));

        NodeTrigger trigger = new NodeTrigger();
        _nodeDiscovery.addListener(trigger);

        curator.setData().forPath(nodePath, BAR.getName().getBytes(Charsets.UTF_8));
        assertTrue(trigger.updatedWithin(10, TimeUnit.SECONDS));

        assertEquals(1, _nodeDiscovery.getNodes().size());
    }

    @Test
    public void testNullValuesRemainValidNodes() throws Exception {
        register(FOO_BUCKET, FOO);
        register(FOO_BUCKET, BAR);

        ZooKeeperNodeDiscovery<Node> nodeDiscovery = new ZooKeeperNodeDiscovery<Node>(
                newZooKeeperConnection(),
                makeBasePath(FOO_BUCKET),
                new NodeDataParser<Node>() {
                    @Override
                    public Node parse(String path, byte[] nodeData) {
                        return null;
                    }
                });
        nodeDiscovery.start();

        assertTrue(waitUntilSize(nodeDiscovery.getNodes(), 2));
        for (Node node : nodeDiscovery.getNodes().values()) {
            assertNull(node);
        }
    }

    @Test
    public void testUpdateOnlyUpdateEventFired() throws Exception {
        String nodePath = ZKPaths.makePath(FOO_BUCKET, "UpdatingNode");
        CuratorFramework curator = newCurator();

        curator.create().creatingParentsIfNeeded().forPath(nodePath, FOO.getName().getBytes(Charsets.UTF_8));
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));

        NodeTrigger trigger = new NodeTrigger();
        _nodeDiscovery.addListener(trigger);

        CountingListener counter = new CountingListener();
        _nodeDiscovery.addListener(counter);

        curator.setData().forPath(nodePath, BAR.getName().getBytes(Charsets.UTF_8));
        assertTrue(trigger.updatedWithin(10, TimeUnit.SECONDS));

        assertEquals(1, _nodeDiscovery.getNodes().size());
        assertEquals(1, counter.getNumUpdates());
    }

    @Test
    public void testParserReturnsValue() throws Exception {
        NodeTrigger trigger = new NodeTrigger(FOO);
        _nodeDiscovery.addListener(trigger);

        register(FOO_BUCKET, FOO);
        trigger.addedWithin(10, TimeUnit.SECONDS);
    }

    @Test
    public void testParserReturnsNull() throws Exception {
        ZooKeeperNodeDiscovery<Node> nodeDiscovery = new ZooKeeperNodeDiscovery<Node>(
                newZooKeeperConnection(),
                makeBasePath(FOO_BUCKET),
                new NodeDataParser<Node>() {
                    @Override
                    public Node parse(String path, byte[] nodeData) {
                        return null;
                    }
                }
        );
        nodeDiscovery.start();

        NodeTrigger trigger = new NodeTrigger((Node) null);
        nodeDiscovery.addListener(trigger);

        register(FOO_BUCKET, FOO);
        trigger.addedWithin(10, TimeUnit.SECONDS);
    }

    @Test
    public void testParserReturnsNullOnException() throws Exception {
        ZooKeeperNodeDiscovery<Node> nodeDiscovery = new ZooKeeperNodeDiscovery<Node>(
                newZooKeeperConnection(),
                makeBasePath(FOO_BUCKET),
                new NodeDataParser<Node>() {
                    @Override
                    public Node parse(String path, byte[] nodeData) {
                        throw new RuntimeException();
                    }
                }
        );
        nodeDiscovery.start();

        NodeTrigger trigger = new NodeTrigger((Node) null);
        nodeDiscovery.addListener(trigger);

        register(FOO_BUCKET, FOO);
        trigger.addedWithin(10, TimeUnit.SECONDS);
    }

    @Test
    public void testAlreadyExistingNodesDoNotFireEvents() throws Exception {
        register(FOO_BUCKET, FOO);

        ZooKeeperNodeDiscovery<Node> nodeDiscovery = new ZooKeeperNodeDiscovery<Node>(
                newZooKeeperConnection(),
                makeBasePath(FOO_BUCKET),
                Node.PARSER
        );
        nodeDiscovery.start();

        assertEquals(1, nodeDiscovery.getNodes().size());

        CountingListener eventCounter = new CountingListener();
        nodeDiscovery.addListener(eventCounter);

        // Don't know when the register() will take effect.  Execute and wait for an
        // unregister--that should be long enough to wait.
        unregister(FOO);
        assertTrue(waitUntilSize(nodeDiscovery.getNodes(), 0));

        assertEquals(0, eventCounter.getNumAdds());  // nodes initially visible never fire add events
    }

    @Test
    public void testNodeRemovedWhenSessionKilled() throws Exception {
        register(FOO_BUCKET, FOO);
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));

        killSession(_nodeDiscovery.getCurator());

        // The entry gets cleaned up because we've lost contact with ZooKeeper
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 0));
    }

    @Test
    public void testNodeReRegisteredWhenSessionKilled() throws Exception {
        register(FOO_BUCKET, FOO);
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));

        killSession(_nodeDiscovery.getCurator());

        // The entry gets cleaned up because we've lost contact with ZooKeeper
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 0));

        // Then it automatically gets created when the connection is re-established with ZooKeeper
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));
    }

    @Test
    public void testRegisterNodeCallsListener() throws Exception {
        NodeTrigger trigger = new NodeTrigger();
        _nodeDiscovery.addListener(trigger);

        register(FOO_BUCKET, FOO);
        assertTrue(trigger.addedWithin(10, TimeUnit.SECONDS));
    }

    @Test
    public void testUnregisterNodeCallsListener() throws Exception {
        NodeTrigger trigger = new NodeTrigger();
        _nodeDiscovery.addListener(trigger);

        register(FOO_BUCKET, FOO);
        assertTrue(trigger.addedWithin(10, TimeUnit.SECONDS));

        unregister(FOO);
        assertTrue(trigger.removedWithin(10, TimeUnit.SECONDS));
    }

    @Test
    public void testRemovedListenerDoesNotSeeEvents() throws Exception {
        NodeTrigger trigger = new NodeTrigger();
        _nodeDiscovery.addListener(trigger);

        CountingListener eventCounter = new CountingListener();
        _nodeDiscovery.addListener(eventCounter);
        _nodeDiscovery.removeListener(eventCounter);

        register(FOO_BUCKET, FOO);
        assertTrue(trigger.addedWithin(10, TimeUnit.SECONDS));

        unregister(FOO);
        assertTrue(trigger.removedWithin(10, TimeUnit.SECONDS));

        assertEquals(0, eventCounter.getNumEvents());
    }

    @Test
    public void testListenerCalledWhenSessionKilled() throws Exception {
        NodeTrigger trigger = new NodeTrigger();
        _nodeDiscovery.addListener(trigger);

        register(FOO_BUCKET, FOO);
        assertTrue(trigger.addedWithin(10, TimeUnit.SECONDS));

        killSession(_nodeDiscovery.getCurator());

        // The entry gets cleaned up because we've lost contact with ZooKeeper
        assertTrue(trigger.removedWithin(10, TimeUnit.SECONDS));
    }

    @Test
    public void testListenerCalledWhenNodeIsReregisteredAfterSessionKilled() throws Exception {
        NodeTrigger initialTrigger = new NodeTrigger();
        _nodeDiscovery.addListener(initialTrigger);

        register(FOO_BUCKET, FOO);
        assertTrue(initialTrigger.addedWithin(10, TimeUnit.SECONDS));

        NodeTrigger trigger = new NodeTrigger();
        _nodeDiscovery.addListener(trigger);

        killSession(_nodeDiscovery.getCurator());

        // The entry gets cleaned up because we've lost contact with ZooKeeper
        assertTrue(trigger.removedWithin(10, TimeUnit.SECONDS));

        // Then it automatically gets created when the connection is re-established with ZooKeeper
        assertTrue(trigger.addedWithin(10, TimeUnit.SECONDS));
    }

    @Test
    public void testMultipleListeners() throws Exception {
        NodeTrigger trigger1 = new NodeTrigger();
        NodeTrigger trigger2 = new NodeTrigger();
        _nodeDiscovery.addListener(trigger1);
        _nodeDiscovery.addListener(trigger2);

        register(FOO_BUCKET, FOO);
        assertTrue(trigger1.addedWithin(10, TimeUnit.SECONDS));
        assertTrue(trigger2.addedWithin(10, TimeUnit.SECONDS));

        unregister(FOO);
        assertTrue(trigger1.removedWithin(10, TimeUnit.SECONDS));
        assertTrue(trigger2.removedWithin(10, TimeUnit.SECONDS));
    }

    @Test
    public void testZooKeeperResetFires() throws Exception {
        NodeTrigger trigger = new NodeTrigger();
        _nodeDiscovery.addListener(trigger);

        killSession(_nodeDiscovery.getCurator());

        assertTrue(trigger.resetWithin(10, TimeUnit.SECONDS));
    }

    @Test
    public void testInitializeRacesRemove() throws Exception {
        // Create a new ZK connection now so it's ready-to-go when we need it.
        ZooKeeperConnection zooKeeperConnection = newZooKeeperConnection();

        // Register FOO and wait until it's visible.
        register(FOO_BUCKET, FOO);
        assertTrue(waitUntilSize(_nodeDiscovery.getNodes(), 1));

        // Unregister FOO and create a new NodeDiscovery instance as close together as we can, so they race.
        unregister(FOO);
        ZooKeeperNodeDiscovery<Node> nodeDiscovery = new ZooKeeperNodeDiscovery<Node>(
                zooKeeperConnection,
                makeBasePath(FOO_BUCKET),
                Node.PARSER
        );
        nodeDiscovery.start();

        assertTrue(waitUntilSize(nodeDiscovery.getNodes(), 0));
    }

    private static <K, T> boolean waitUntilSize(Map<K, T> map, int size, long timeout, TimeUnit unit) {
        long start = System.nanoTime();
        while (System.nanoTime() - start <= unit.toNanos(timeout)) {
            if (map.size() == size) {
                return true;
            }

            Thread.yield();
        }

        return false;
    }

    private static <K, T> boolean waitUntilSize(Map<K, T> map, int size) {
        return waitUntilSize(map, size, 10, TimeUnit.SECONDS);
    }

    private static final class NodeTrigger implements NodeListener<Node> {
        private final Optional<Node> _expected;

        private final Trigger _addTrigger = new Trigger();
        private final Trigger _removeTrigger = new Trigger();
        private final Trigger _updateTrigger = new Trigger();
        private final Trigger _resetTrigger = new Trigger();

        public NodeTrigger() {
            this(Optional.<Node>absent());
        }

        public NodeTrigger(Node expected) {
            this(Optional.fromNullable(expected));
        }

        public NodeTrigger(Optional<Node> expected) {
            checkNotNull(expected);

            _expected = expected;
        }

        @Override
        public void onNodeAdded(String path, Node node) {
            if (!_expected.isPresent() || Objects.equal(_expected, node)) {
                _addTrigger.fire();
            }
        }

        @Override
        public void onNodeRemoved(String path, Node node) {
            if (!_expected.isPresent() || Objects.equal(_expected, node)) {
                _removeTrigger.fire();
            }
        }

        @Override
        public void onNodeUpdated(String path, Node node) {
            if (!_expected.isPresent() || Objects.equal(_expected, node)) {
                _updateTrigger.fire();
            }
        }

        @Override
        public void onZooKeeperReset() {
            _resetTrigger.fire();
        }

        public boolean addedWithin(long duration, TimeUnit unit) throws InterruptedException {
            return _addTrigger.firedWithin(duration, unit);
        }

        public boolean removedWithin(long duration, TimeUnit unit) throws InterruptedException {
            return _removeTrigger.firedWithin(duration, unit);
        }

        public boolean updatedWithin(long duration, TimeUnit unit) throws InterruptedException {
            return _updateTrigger.firedWithin(duration, unit);
        }

        public boolean resetWithin(long duration, TimeUnit unit) throws InterruptedException {
            return _resetTrigger.firedWithin(duration, unit);
        }
    }

    private static final class CountingListener implements NodeListener<Node> {
        private int _numAdds;
        private int _numRemoves;
        private int _numUpdates;
        private int _numResets;

        @Override
        public void onNodeAdded(String path, Node node) {
            _numAdds++;
        }

        @Override
        public void onNodeRemoved(String path, Node node) {
            _numRemoves++;
        }

        @Override
        public void onNodeUpdated(String path, Node node) {
            _numUpdates++;
        }

        @Override
        public void onZooKeeperReset() {
            _numResets++;
        }

        public int getNumAdds() {
            return _numAdds;
        }

        public int getNumRemoves() {
            return _numRemoves;
        }

        public int getNumResets() {
            return _numResets;
        }

        public int getNumUpdates() {
            return _numUpdates;
        }

        public int getNumEvents() {
            return _numAdds + _numRemoves + _numUpdates + _numResets;
        }
    }
}
