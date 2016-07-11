package com.alibaba.middleware.race.model;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;

import java.util.Map;
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
        if (prefix.equals(RaceConfig.prex_tmall)) {
            long a = 1;
        } else {
            long a = 1;
        }
        Amount amountObj = amountMap.get(timeStamp);
        if (amountObj == null) {
            amountObj = new Amount(timeStamp,prefix);
            amountMap.put(timeStamp,amountObj);
        }
        amountObj.updateAmount(amount);
    }

    // todo 记得去掉
//    public void writeTair(long timeStamp,int count,int flag) throws InterruptedException {
//        Amount amount = amountMap.get(timeStamp);
//        // 这个地方
//        if (!toTairQueue.contains(amount))
//            toTairQueue.offer(amount);
//        if (count == 200257 || count == 199945) {
//            for (Map.Entry<Long, Amount> entry : amountMap.entrySet()) {
//                System.out.println("***** " + entry.getKey() +": " + entry.getValue().getSumAmount() +"," + flag+","+timeStamp);
//            }
//        }
//    }

    public void writeTair(long timeStamp) throws InterruptedException {
        Amount amount = amountMap.get(timeStamp);
        // 这个地方
        if (!toTairQueue.contains(amount))
            toTairQueue.offer(amount);
    }
}
