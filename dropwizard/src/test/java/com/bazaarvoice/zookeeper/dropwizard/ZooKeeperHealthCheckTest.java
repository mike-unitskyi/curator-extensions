package com.bazaarvoice.zookeeper.dropwizard;

import com.bazaarvoice.zookeeper.internal.CuratorConnection;
import org.junit.Test;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ZooKeeperHealthCheckTest {
    @Test
    public void testNotConnectedIsUnhealthy() throws Exception {
        CuratorConnection curatorConnection = mock(CuratorConnection.class, RETURNS_DEEP_STUBS);
        when(curatorConnection.getCurator().getZookeeperClient().isConnected()).thenReturn(false);

        ZooKeeperHealthCheck healthCheck = new ZooKeeperHealthCheck(curatorConnection);
        assertFalse(healthCheck.check().isHealthy());
    }

    @Test
    public void testConnectedIsHealthy() throws Exception {
        CuratorConnection curatorConnection = mock(CuratorConnection.class, RETURNS_DEEP_STUBS);
        when(curatorConnection.getCurator().getZookeeperClient().isConnected()).thenReturn(true);

        ZooKeeperHealthCheck healthCheck = new ZooKeeperHealthCheck(curatorConnection);
        assertTrue(healthCheck.check().isHealthy());
    }
}
