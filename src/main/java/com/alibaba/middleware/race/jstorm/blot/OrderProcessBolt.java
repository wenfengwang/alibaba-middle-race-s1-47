package com.alibaba.middleware.race.jstorm.blot;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by sxian.wang on 2016/7/9.
 */
public class OrderProcessBolt implements IBasicBolt, Serializable {

    public static final HashSet<Long> TBOrderSet = new HashSet<>();
    public static final HashSet<Long> TMOrderSet = new HashSet<>();
    public static final LinkedBlockingQueue<ArrayList<Long>> TBOrderQueue = new LinkedBlockingQueue<>();
    public static final LinkedBlockingQueue<ArrayList<Long>> TMOrderQueue = new LinkedBlockingQueue<>();

    // 这几个数据结构线程安全的问题
    public static final LinkedBlockingQueue<Object[]> unfindedOrder = new LinkedBlockingQueue<>();
    public static final LinkedBlockingQueue<Object[]> tbWaittingEmitList = new LinkedBlockingQueue<>();
    public static final LinkedBlockingQueue<Object[]> tmWaittingEmitList = new LinkedBlockingQueue<>();
    public static final AtomicBoolean isRunningTbWaitList = new AtomicBoolean(false);
    public static final AtomicBoolean isRunningTmWaitList = new AtomicBoolean(false);

    private transient ExecutorService waitMsgFixedThreadPool;
    private transient ExecutorService paymenyMsgFixedThreadPool;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        waitMsgFixedThreadPool = Executors.newFixedThreadPool(2);
        paymenyMsgFixedThreadPool = Executors.newFixedThreadPool(8);
        for (int i = 0;i<2;i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            Object[] obj = unfindedOrder.take();
                            if (TBOrderSet.contains(obj[1])) {
                                tbWaittingEmitList.offer(new Object[]{obj[0],obj[2]}); // 时间戳 金额
                            } else if (TMOrderSet.contains(obj[1])){
                                tmWaittingEmitList.offer(new Object[]{obj[0],obj[2]});
                            } else {
                                unfindedOrder.offer(obj);
                            }
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }).start();
        }
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        TBOrderSet.addAll(TBOrderQueue.take());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        TMOrderSet.addAll(TMOrderQueue.take());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        processTuple(input.getSourceComponent(),input.getValues(),collector);
    }

    private void processTuple(final String boltId, final List<Object> list, final BasicOutputCollector collector) {
        paymenyMsgFixedThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                switch (boltId) {
                    case RaceConfig.RATIO_SPLIT_BOLT_ID:
                        long timeStamp = (long) list.get(0);
                        long orderId = (long) list.get(1);
                        double price = (double) list.get(2);
                        if (timeStamp == 0 && orderId == 0 && price == 0) {
                            collector.emit(RaceConfig.TAOBAO_PERSIST_STREAM_ID, new Values(0l, 0.0));
                            collector.emit(RaceConfig.TMALL_PERSIST_STREAM_ID, new Values(0l, 0.0));
                            return;
                        }
                        if (TBOrderSet.contains(orderId)) {
                            collector.emit(RaceConfig.TAOBAO_PERSIST_STREAM_ID, new Values(timeStamp, price));
                            emitWaittingMsg(1,collector);
                        } else if (TMOrderSet.contains(orderId)) {
                            collector.emit(RaceConfig.TMALL_PERSIST_STREAM_ID, new Values(timeStamp, price));
                            emitWaittingMsg(2,collector);
                        } else {
                            unfindedOrder.offer(new Object[]{timeStamp,orderId,price});
                        }
                        break;
                    case RaceConfig.TAOBAO_COUNT_BOLT_ID:
                        TBOrderQueue.add((ArrayList<Long>) list.get(0));
                        break;
                    case RaceConfig.TMALL_COUNT_BOLT_ID:
                        TMOrderQueue.add((ArrayList<Long>) list.get(0));
                        break;
                    default:
                        break;
                }
            }
        });

    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(RaceConfig.TAOBAO_PERSIST_STREAM_ID,new Fields("timestamp","amount"));
        declarer.declareStream(RaceConfig.TMALL_PERSIST_STREAM_ID,new Fields("timestamp","amount"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private void emitWaittingMsg(int flag, final BasicOutputCollector collector) {
        if (flag == 1 && !isRunningTbWaitList.get()) {
            waitMsgFixedThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    Object[] obj = tbWaittingEmitList.poll();
                    isRunningTbWaitList.set(true);
                    while (obj!=null) {
                        collector.emit(RaceConfig.TAOBAO_PERSIST_STREAM_ID, new Values(obj[0],obj[1]));
                        obj = tbWaittingEmitList.poll();
                    }
                    isRunningTbWaitList.set(false);
                }
            });
        } else if (!isRunningTmWaitList.get()){
            waitMsgFixedThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    Object[] obj = tmWaittingEmitList.poll();
                    isRunningTmWaitList.set(true);
                    while (obj != null) {
                        collector.emit(RaceConfig.TMALL_PERSIST_STREAM_ID, new Values(obj[0],obj[1]));
                        obj = tmWaittingEmitList.poll();
                    }
                    isRunningTmWaitList.set(false);
                }
            });
        }
    }
}
