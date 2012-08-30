package com.bazaarvoice.zookeeper.internal;

import org.apache.zookeeper.common.PathUtils;

class Namespaces {

    static String normalize(String namespace) {
        // Pass null to Curator to indicate no namespace, but then CuratorFramework.getNamespace() will return the
        // empty string "".  So coerce the empty string to null to mean no namespace.
        if (namespace == null || namespace.isEmpty() || "/".equals(namespace)) {
            return null;
        }

        // Verify the namespace is well formed based on ZooKeeper rules for path names.
        PathUtils.validatePath(namespace);

        return namespace;
    }
}
