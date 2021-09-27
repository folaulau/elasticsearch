package com.kaveinga.elasticsearch.status;

import java.util.Arrays;

import com.kaveinga.elasticsearch.utils.RandomGeneratorUtils;

public enum UserStatus {

    ACTIVE,
    SUSPENDED,
    INACTIVE,
    CANCELLED;

    public static UserStatus getRandomStatus() {
        UserStatus[] statuses = UserStatus.values();
        return statuses[RandomGeneratorUtils.getIntegerWithin(0, statuses.length)];
    }
}
