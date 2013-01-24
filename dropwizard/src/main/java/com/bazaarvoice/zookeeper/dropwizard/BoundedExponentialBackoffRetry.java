package com.bazaarvoice.zookeeper.dropwizard;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BoundedExponentialBackoffRetry {
    public final int baseSleepTimeMs;
    public final int maxSleepTimeMs;
    public final int maxAttempts;

    @JsonCreator
    public BoundedExponentialBackoffRetry(@JsonProperty("baseSleepTimeMs") int baseSleepTimeMs,
                                          @JsonProperty("maxSleepTimeMs") int maxSleepTimeMs,
                                          @JsonProperty("maxAttempts") int maxAttempts) {
        this.baseSleepTimeMs = baseSleepTimeMs;
        this.maxSleepTimeMs = maxSleepTimeMs;
        this.maxAttempts = maxAttempts;
    }
}
