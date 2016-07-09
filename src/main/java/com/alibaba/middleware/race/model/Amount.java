package com.alibaba.middleware.race.model;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.google.common.util.concurrent.AtomicDouble;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by sxian.wang on 2016/7/9.
 */
public class Amount implements Serializable {
    public final long timeStamp;
    private final String key;

    private volatile AtomicDouble amount = new AtomicDouble(0);

    public Amount(long timeStamp,String prefix) {
        this.timeStamp = timeStamp;
        key = prefix + timeStamp;
    }

    public void updateAmount(double amount) {
        this.amount.addAndGet(amount);
    }

    public void toTair(TairOperatorImpl tairOperator) {
        tairOperator.write(key, amount);
    }
}
