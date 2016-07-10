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
        for (int i = 0;i<2;i++) {  // todo 关注下会不会有线程安全问题
            new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            Ratio ratio = ratioQueue.take();
                            ratio.toTair(tairOperator, ratioQueue);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }).start();
        }
    }

    public void updateAmount(final Ratio ratio, final double[] amount) {
        // 多线程更新的问题
        fixedThread.execute(new Runnable() {
            @Override
            public void run() {
                ratio.updateCurrentAmount(amount);
            }
        });
    }

    public void updateRatio(Ratio ratio) {
        if (!ratioQueue.contains(ratio)) {
            ratioQueue.offer(ratio);
        }
    }
}
