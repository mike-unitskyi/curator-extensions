package com.bazaarvoice.zookeeper.recipes.discovery;

import com.bazaarvoice.zookeeper.test.ZooKeeperTest;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Optional;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class Node {
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

    public static final NodeDataParser<Node> PARSER = new NodeDataParser<Node>() {
        @Override
        public Node parse(String path, byte[] data) {
            return new Node(new String(data, Charsets.UTF_8));
        }
    };

    public static final class NodeTrigger implements NodeListener<Node> {
        private final Optional<Node> _expected;

        private final ZooKeeperTest.Trigger _addTrigger = new ZooKeeperTest.Trigger();
        private final ZooKeeperTest.Trigger _removeTrigger = new ZooKeeperTest.Trigger();
        private final ZooKeeperTest.Trigger _updateTrigger = new ZooKeeperTest.Trigger();
        private final ZooKeeperTest.Trigger _suspendedTrigger = new ZooKeeperTest.Trigger();
        private final ZooKeeperTest.Trigger _lostTrigger = new ZooKeeperTest.Trigger();
        private final ZooKeeperTest.Trigger _reconnectedTrigger = new ZooKeeperTest.Trigger();

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
        public void onZooKeeperEvent(ZooKeeperEvent event) {
            switch (event) {
                case SUSPENDED:
                    _suspendedTrigger.fire();
                    break;
                case LOST:
                    _lostTrigger.fire();
                    break;
                case RECONNECTED:
                    _reconnectedTrigger.fire();
                    break;
            }
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

        public boolean suspendedWithin(long duration, TimeUnit unit) throws InterruptedException {
            return _suspendedTrigger.firedWithin(duration, unit);
        }

        public boolean lostWithin(long duration, TimeUnit unit) throws InterruptedException {
            return _lostTrigger.firedWithin(duration, unit);
        }

        public boolean reconnectedWithin(long duration, TimeUnit unit) throws InterruptedException {
            return _reconnectedTrigger.firedWithin(duration, unit);
        }
    }

    public static final class CountingListener implements NodeListener<Node> {
        private int _numAdds;
        private int _numRemoves;
        private int _numUpdates;

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
        public void onZooKeeperEvent(ZooKeeperEvent event) {
        }

        public int getNumAdds() {
            return _numAdds;
        }

        public int getNumRemoves() {
            return _numRemoves;
        }

        public int getNumUpdates() {
            return _numUpdates;
        }

        public int getNumEvents() {
            return _numAdds + _numRemoves + _numUpdates;
        }
    }
}
