package com.bazaarvoice.zookeeper.recipes.discovery;

import com.netflix.curator.framework.recipes.cache.ChildData;

public interface ChildDataParser<T> {
    T parse(ChildData childData);
}
