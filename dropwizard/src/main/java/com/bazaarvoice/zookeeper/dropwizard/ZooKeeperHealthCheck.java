package com.bazaarvoice.zookeeper.dropwizard;

import com.bazaarvoice.zookeeper.ZooKeeperConnection;
import com.bazaarvoice.zookeeper.internal.CuratorConnection;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;
import com.yammer.metrics.core.HealthCheck;

import java.util.concurrent.atomic.AtomicReference;

/**
 * This is a Dropwizard compatible healthcheck for ZooKeeper.
 **/
public class ZooKeeperHealthCheck extends HealthCheck {
    private final CuratorFramework _curator;

    public ZooKeeperHealthCheck(ZooKeeperConnection connection) {
        super("zookeeper");

        _curator = ((CuratorConnection) connection).getCurator();
    }

    @Override
    protected Result check() throws Exception {
        boolean connected = _curator.getZookeeperClient().isConnected();

        String description = String.format(
            "Connect String: %s Current State: %s",
            _curator.getZookeeperClient().getCurrentConnectionString(),
            connected
        );

        if (!connected) {
            return Result.unhealthy(description);
        }

        return Result.healthy(description);
    }
}
