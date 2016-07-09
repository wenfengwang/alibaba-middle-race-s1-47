package com.alibaba.middleware.race.model;

import com.alibaba.middleware.race.Tair.TairOperatorImpl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by sxian.wang on 2016/7/8.
 */
public class RatioProcess {
    public static final ExecutorService fixedThread = Executors.newFixedThreadPool(1);
    private final TairOperatorImpl tairOperator = new TairOperatorImpl();
    private final LinkedBlockingQueue<Ratio> ratioQueue = new LinkedBlockingQueue<>();

    public RatioProcess() {
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
