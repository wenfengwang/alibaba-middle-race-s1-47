package com.alibaba.middleware.race.model;

import com.alibaba.middleware.race.Tair.TairOperatorImpl;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by sxian.wang on 2016/7/9.
 */
public class Amount implements Serializable {
    public final long timeStamp;
    private final String key;

    private double sumAmount = 0;  // todo 去掉 volatile 和原子类就好了？

    public Amount(long timeStamp,String prefix) {
        this.timeStamp = timeStamp;
        key = prefix + timeStamp;
    }

    public void updateAmount(double amount) {
        sumAmount += amount;
    }

    public void writeTair(TairOperatorImpl tairOperator) {
        tairOperator.write(key, sumAmount);
    }
}
