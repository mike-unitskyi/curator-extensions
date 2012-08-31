package com.bazaarvoice.zookeeper.internal;

import com.bazaarvoice.zookeeper.ZooKeeperConnection;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.utils.ZKPaths;

import java.io.IOException;
import java.util.concurrent.ThreadFactory;

import static com.google.common.base.Preconditions.checkNotNull;

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
        namespace = Namespaces.normalize(namespace);

        // Make all of the curator threads daemon threads so they don't block the JVM from terminating.
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("CuratorFramework[" + connectString + "]-%d")
                .setDaemon(true)
                .build();

        _curator = builder.connectString(connectString)
                .retryPolicy(retryPolicy)
                .namespace(namespace)
                .threadFactory(threadFactory)
                .build();
        _curator.start();
    }

    /**
     * Private constructor for creating namespaced curator instances.
     */
    private CuratorConnection(CuratorFramework curator) {
        _curator = curator;
    }

    @Override
    public ZooKeeperConnection withNamespace(String namespace) {
        String relative = Namespaces.normalize(namespace);
        if (relative == null) {
            return this;
        }
        String parent = Strings.emptyToNull(_curator.getNamespace());
        String absolute = ZKPaths.fixForNamespace(parent, relative);
        return new CuratorConnection(_curator.usingNamespace(absolute));
    }

    public CuratorFramework getCurator() {
        return _curator;
    }

    @Override
    public void close() throws IOException {
        _curator.close();
    }
}
