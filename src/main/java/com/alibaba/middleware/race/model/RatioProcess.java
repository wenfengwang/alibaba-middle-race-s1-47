package com.alibaba.middleware.race.model;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;

import java.util.concurrent.*;

/**
 * Created by sxian.wang on 2016/7/8.
 */
public class RatioProcess {
    public static final ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
    public static final ScheduledExecutorService scheduleThreadPool = Executors.newScheduledThreadPool(2);
    public static final LinkedBlockingQueue<Ratio> updateAmountQueue = new LinkedBlockingQueue();
    public static final LinkedBlockingQueue<Ratio> updateRatioQueue = new LinkedBlockingQueue();
    private final TairOperatorImpl tairOperator;

    public RatioProcess() {
        tairOperator = new TairOperatorImpl(RaceConfig.TairServerAddr,RaceConfig.TairNamespace);

        scheduleThreadPool.schedule(new Runnable() {

            @Override
            public void run() {
                try {
                    Ratio ratio = updateAmountQueue.take();

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        },100, TimeUnit.MILLISECONDS);
    }

    public void updateAmount(final Ratio ratio, final double[] amount) {
        cachedThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                ratio.updateCurrentAmount(amount);
            }
        });
    }

    public void updateRatio(final Ratio ratio) {
        cachedThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                ratio.toTair(tairOperator);
            }
        });
    }
}
