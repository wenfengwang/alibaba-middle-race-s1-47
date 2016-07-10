package com.alibaba.middleware.race.model;

import com.alibaba.middleware.race.Tair.TairOperatorImpl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by sxian.wang on 2016/7/9.
 */
public class AmountProcess {
    public  final ConcurrentHashMap<Long,Amount> amountMap = new ConcurrentHashMap<>();
    private final int THREAD_NUM = 5;
    private final LinkedBlockingQueue<Amount> toTairQueue = new LinkedBlockingQueue<>();


    public AmountProcess() {
        for (int i = 0; i < THREAD_NUM; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    TairOperatorImpl tairOperator = new TairOperatorImpl();
                    try {
                        while (true) {
                            Amount amount = toTairQueue.take();
                            amount.writeTair(tairOperator);
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
    }

    public void updateAmount(final long timeStamp,final double amount,String prefix) {
        if (timeStamp == 0) {
            System.out.println();
        }
        Amount amountObj = amountMap.get(timeStamp);
        if (amountObj == null) {
            amountObj = new Amount(timeStamp,prefix);
            amountMap.put(timeStamp,amountObj);
        }
        amountObj.updateAmount(amount);
    }

    public void writeTair(long timeStamp) throws InterruptedException {
        if (timeStamp == 0) {
            System.out.println();
        }
        Amount amount = amountMap.get(timeStamp);
        synchronized (toTairQueue) {
            if (!toTairQueue.contains(amount))
                toTairQueue.offer(amount);
        }
    }
}
