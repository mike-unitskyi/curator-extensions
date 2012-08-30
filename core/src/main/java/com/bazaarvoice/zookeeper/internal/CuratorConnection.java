package com.bazaarvoice.zookeeper.internal;

import com.bazaarvoice.zookeeper.ZooKeeperConnection;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.utils.ZKPaths;

import java.io.IOException;
import java.util.concurrent.ThreadFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * <b>NOTE: This is an INTERNAL class to the SOA library.  You should not be using this directly!!!!</b>
 *
 * This class provides a ZooKeeperConnection that is backed by Netflix's Curator library.  This class is in an internal
 * package to avoid users from using it.  Curator should not appear anywhere in the public interface of the SOA library.
 *
 * @see com.bazaarvoice.zookeeper.ZooKeeperConfiguration
 */
public class CuratorConnection implements ZooKeeperConnection {
    private final CuratorFramework _curator;
    private final String _namespace;

    public CuratorConnection(String connectString, RetryPolicy retryPolicy, String namespace) {
        this(connectString, retryPolicy, namespace, CuratorFrameworkFactory.builder());
    }

    @VisibleForTesting
    CuratorConnection(String connectString, RetryPolicy retryPolicy, String namespace,
                      CuratorFrameworkFactory.Builder builder) {
        checkNotNull(connectString);
        checkNotNull(retryPolicy);
        checkNotNull(builder);

        // Sanitize the namespace and verify it is valid
        _namespace = Namespaces.normalize(namespace);

        // Make all of the curator threads daemon threads so they don't block the JVM from terminating.
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("CuratorFramework[" + connectString + "]-%d")
                .setDaemon(true)
                .build();

        try {
            _curator = builder
                    .connectString(connectString)
                    .retryPolicy(retryPolicy)
                    .namespace(_namespace)
                    .threadFactory(threadFactory)
                    .build();
            _curator.start();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Private constructor for creating namespaced curator instances.
     */
    private CuratorConnection(CuratorFramework curator) {
        checkState(!curator.getNamespace().isEmpty());
        _curator = curator;
        _namespace = curator.getNamespace();
    }

    @Override
    public ZooKeeperConnection withNamespace(String namespace) {
        String relative = Namespaces.normalize(namespace);
        if (relative == null) {
            return this;
        }
        String absolute = ZKPaths.fixForNamespace(_namespace, relative);
        return new CuratorConnection(_curator.usingNamespace(absolute));
    }

    public CuratorFramework getCurator() {
        return _curator;
    }

    public String getNamespace() {
        // Temporary workaround for the fact that CuratorFrameworkImpl.getNamespace() always returns "" even when
        // CuratorFrameworkImpl.namespace is non-empty.  Ideally this should return _curator.getNamespace().
        return _namespace != null ? _namespace : "";
    }

    @Override
    public void close() throws IOException {
        _curator.close();
    }
}
