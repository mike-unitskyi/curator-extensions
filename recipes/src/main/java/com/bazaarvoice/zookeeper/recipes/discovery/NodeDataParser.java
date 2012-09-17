package com.bazaarvoice.zookeeper.recipes.discovery;

public interface NodeDataParser<T> {
    T parse(byte[] nodeData);
}
