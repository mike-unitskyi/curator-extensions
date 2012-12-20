package com.bazaarvoice.zookeeper.dropwizard;

import com.bazaarvoice.zookeeper.ZooKeeperConnection;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ManagedZooKeeperConnectionTest {
    @Test
    public void testClosesOnStop() throws Exception {
        ZooKeeperConnection zookeeper = mock(ZooKeeperConnection.class);

        ManagedZooKeeperConnection managed = new ManagedZooKeeperConnection(zookeeper);
        managed.stop();

        verify(zookeeper).close();
    }
}
