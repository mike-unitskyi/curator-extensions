package com.bazaarvoice.curator.dropwizard;

import org.junit.Test;
import sun.net.spi.nameservice.NameService;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ResolvingEnsembleProviderTest {
    private final ResolvingEnsembleProvider.Resolver _resolver = mock(ResolvingEnsembleProvider.Resolver.class);

    @Test
    public void testNameResolves() throws Exception {
        when(_resolver.lookupAllHostAddr("test"))
                .thenReturn(new InetAddress[] {InetAddress.getByAddress(new byte[] {1, 1, 1, 1})});

        ResolvingEnsembleProvider provider = new ResolvingEnsembleProvider("test:2181", _resolver);

        assertEquals("1.1.1.1:2181", provider.getConnectionString());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testNameDoesNotResolve() throws Exception {
        when(_resolver.lookupAllHostAddr("test")).thenThrow(UnknownHostException.class);

        ResolvingEnsembleProvider provider = new ResolvingEnsembleProvider("test:2181", _resolver);

        assertEquals("test:2181", provider.getConnectionString());
    }

    @Test
    public void testMultipleRecords() throws Exception {
        when(_resolver.lookupAllHostAddr("test"))
                        .thenReturn(new InetAddress[] {
                                InetAddress.getByAddress(new byte[] {1, 1, 1, 1}),
                                InetAddress.getByAddress(new byte[] {2, 2, 2, 2})
                        });

        ResolvingEnsembleProvider provider = new ResolvingEnsembleProvider("test:2181", _resolver);

        assertEquals("1.1.1.1:2181,2.2.2.2:2181", provider.getConnectionString());
    }

    @Test
    public void testMultipleRecordsCanonical() throws Exception {
        // Return records in different order to simulate round robin DNS.
        when(_resolver.lookupAllHostAddr("test"))
                                .thenReturn(new InetAddress[] {
                                        InetAddress.getByAddress(new byte[] {1, 1, 1, 1}),
                                        InetAddress.getByAddress(new byte[] {2, 2, 2, 2})
                                })
                                .thenReturn(new InetAddress[] {
                                        InetAddress.getByAddress(new byte[] {2, 2, 2, 2}),
                                        InetAddress.getByAddress(new byte[] {1, 1, 1, 1})
                                });

        ResolvingEnsembleProvider provider = new ResolvingEnsembleProvider("test:2181", _resolver);

        // Should provide the same connect string regardless of DNS order.
        assertEquals("1.1.1.1:2181,2.2.2.2:2181", provider.getConnectionString());
        assertEquals("1.1.1.1:2181,2.2.2.2:2181", provider.getConnectionString());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testChrootPath() throws Exception {
        when(_resolver.lookupAllHostAddr("test")).thenThrow(UnknownHostException.class);

                ResolvingEnsembleProvider provider = new ResolvingEnsembleProvider("test:2181/chroot", _resolver);

                assertEquals("test:2181/chroot", provider.getConnectionString());
    }
}
