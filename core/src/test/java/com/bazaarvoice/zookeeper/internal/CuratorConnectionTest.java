package com.bazaarvoice.zookeeper.internal;

import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CuratorConnectionTest {
    private final CuratorFramework _curator = mock(CuratorFramework.class);
    private CuratorFrameworkFactory.Builder _builder;
    private final String _connectString = "localhost:2181";
    private final RetryPolicy _retry = mock(RetryPolicy.class);
    private final String _namespace = "/test";

    @Before
    public void setup() throws IOException {
        // Return the mock on builder style chainable methods.
        _builder = mock(CuratorFrameworkFactory.Builder.class, new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object mock = invocationOnMock.getMock();
                if (invocationOnMock.getMethod().getReturnType().isInstance(mock)) {
                    return mock;
                } else {
                    return Mockito.RETURNS_DEFAULTS.answer(invocationOnMock);
                }
            }
        });
        when(_builder.build()).thenReturn(_curator);
    }

    @Test(expected = NullPointerException.class)
    public void testNullConnectString() {
        new CuratorConnection(null, _retry, _namespace, _builder);
    }

    @Test(expected = NullPointerException.class)
    public void testNullRetry() {
        new CuratorConnection(_connectString, null, _namespace, _builder);
    }

    @Test
    public void testNullNamespace() {
        new CuratorConnection(_connectString, _retry, null, _builder);
    }

    @Test(expected = NullPointerException.class)
    public void testNullBuilder() {
        new CuratorConnection(_connectString, _retry, _namespace, null);
    }

    @Test(expected = RuntimeException.class)
    public void testBuilderException() throws IOException {
        when(_builder.build()).thenThrow(new IOException());

        new CuratorConnection(_connectString, _retry, _namespace, _builder);
    }

    @Test
    public void testEmptyNamespace() {
        new CuratorConnection(_connectString, _retry, "", _builder);

        verify(_builder).namespace(null);
    }

    @Test
    public void testConnectString() {
        new CuratorConnection(_connectString, _retry, _namespace, _builder);

        verify(_builder).connectString(_connectString);
    }

    @Test
    public void testRetry() {
        new CuratorConnection(_connectString, _retry, _namespace, _builder);

        verify(_builder).retryPolicy(_retry);
    }

    @Test
    public void testNamespace() {
        new CuratorConnection(_connectString, _retry, _namespace, _builder);

        verify(_builder).namespace(_namespace);
    }

    @Test
    public void testGetCurator() {
        CuratorConnection connection = new CuratorConnection(_connectString, _retry, _namespace, _builder);

        assertSame(_curator, connection.getCurator());
    }

    @Test
    public void testClose() throws IOException {
        CuratorConnection connection = new CuratorConnection(_connectString, _retry, _namespace, _builder);

        connection.close();

        verify(_curator).close();
    }
}
