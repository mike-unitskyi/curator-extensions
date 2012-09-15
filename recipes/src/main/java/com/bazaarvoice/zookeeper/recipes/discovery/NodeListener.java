package com.bazaarvoice.zookeeper.recipes.discovery;

public interface NodeListener<T> {
    void onNodeAdded(T node);
    void onNodeRemoved(T node);
}
