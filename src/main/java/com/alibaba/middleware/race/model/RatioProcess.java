package com.alibaba.middleware.race.model;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;

import java.util.HashMap;
import java.util.concurrent.*;

/**
 * Created by sxian.wang on 2016/7/8.
 */
public class RatioProcess {
    public static final ExecutorService fixedThread = Executors.newFixedThreadPool(2);
    private final TairOperatorImpl tairOperator;
//    private final ConcurrentHashMap<Long,Double> amountMap = new ConcurrentHashMap<>();
//    private final ConcurrentHashMap<Long,Double> amountMap = new ConcurrentHashMap<>();
    private final LinkedBlockingQueue<Ratio> ratioQueue = new LinkedBlockingQueue<>();

    public RatioProcess() {
        tairOperator = new TairOperatorImpl(RaceConfig.TairServerAddr,RaceConfig.TairNamespace);
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Ratio ratio = ratioQueue.take();
                        ratio.toTair(tairOperator);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }

    public void updateAmount(final Ratio ratio, final double[] amount) {
//        ratio.naiveUpdateAmount(amount);
        fixedThread.execute(new Runnable() {
            @Override
            public void run() {
                ratio.updateCurrentAmount(amount);
            }
        });
    }

    public void updateRatio(final Ratio ratio) {
        ratioQueue.offer(ratio);
//        fixedThread.execute(new Runnable() {
//            @Override
//            public void run() {
//                ratio.toTair(tairOperator);
//            }
//        });
    }
}
