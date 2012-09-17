package com.bazaarvoice.zookeeper.recipes.discovery;

public interface NodeListener<T> {
    void onNodeAdded(String path, T node);
    void onNodeRemoved(String path, T node);
    void onNodeUpdated(String path, T node);
}
