package com.bazaarvoice.zookeeper.recipes.discovery;

import com.bazaarvoice.zookeeper.test.ZooKeeperTest;
import com.google.common.base.Objects;
import com.google.common.base.Optional;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public final class NodeTrigger implements NodeListener<Node> {
    private final Optional<Node> _expected;

    private final ZooKeeperTest.Trigger _addTrigger = new ZooKeeperTest.Trigger();
    private final ZooKeeperTest.Trigger _removeTrigger = new ZooKeeperTest.Trigger();
    private final ZooKeeperTest.Trigger _updateTrigger = new ZooKeeperTest.Trigger();

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

    public boolean addedWithin(long duration, TimeUnit unit) throws InterruptedException {
        return _addTrigger.firedWithin(duration, unit);
    }

    public boolean removedWithin(long duration, TimeUnit unit) throws InterruptedException {
        return _removeTrigger.firedWithin(duration, unit);
    }

    public boolean updatedWithin(long duration, TimeUnit unit) throws InterruptedException {
        return _updateTrigger.firedWithin(duration, unit);
    }
}
