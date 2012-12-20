package com.bazaarvoice.zookeeper.dropwizard;

import com.bazaarvoice.zookeeper.ZooKeeperConnection;
import com.yammer.dropwizard.lifecycle.Managed;

import static com.google.common.base.Preconditions.checkNotNull;

public class ManagedZooKeeperConnection implements Managed {
    private final ZooKeeperConnection _zookeeper;

    public ManagedZooKeeperConnection(ZooKeeperConnection zookeeper) {
        _zookeeper = checkNotNull(zookeeper);
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void stop() throws Exception {
        _zookeeper.close();
    }
}
