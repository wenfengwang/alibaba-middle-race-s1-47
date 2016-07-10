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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by sxian.wang on 2016/7/9.
 */
public class OrderProcessBolt implements IBasicBolt, Serializable {

    public static final HashSet<Long> TBOrderSet = new HashSet<>();
    public static final HashSet<Long> TMOrderSet = new HashSet<>();
    public static final LinkedBlockingQueue<Object[]> unfindedOrder = new LinkedBlockingQueue<>();

    public static final LinkedList<Object[]> tbWaittingEmitList = new LinkedList<>();
    public static final LinkedList<Object[]> tmWaittingEmitList = new LinkedList<>();

    private transient ExecutorService fixedThreadPool;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        fixedThreadPool = Executors.newFixedThreadPool(2);
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


    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

        switch (input.getSourceComponent()) {
            case RaceConfig.RATIO_SPLIT_BOLT_ID:
                long timeStamp = (long) input.getValue(0);
                long orderId = (long) input.getValue(1);
                double price = (double) input.getValue(2);
                if (timeStamp == 0 && orderId == 0 && price == 0) {
                    collector.emit(RaceConfig.TAOBAO_PERSIST_STREAM_ID, new Values(0, 0));
                    collector.emit(RaceConfig.TMALL_PERSIST_STREAM_ID, new Values(0, 0));

                }
                if (TBOrderSet.contains(orderId)) {
                    collector.emit(RaceConfig.TAOBAO_PERSIST_STREAM_ID, new Values(timeStamp, price));
                    emitWaittingMsg(1,collector);
                } else if (TMOrderSet.contains(orderId)){
                    collector.emit(RaceConfig.TMALL_PERSIST_STREAM_ID, new Values(timeStamp, price));
                    emitWaittingMsg(2,collector);
                } else {
                    unfindedOrder.offer(new Object[]{timeStamp,orderId,price});
                }
                break;
            case RaceConfig.TAOBAO_COUNT_BOLT_ID:
                // TODO 如果这样效率很低，何以考虑用时间戳做key，HashSet作为vale，建索引
                TBOrderSet.addAll((ArrayList<Long>) input.getValue(0));
                break;
            case RaceConfig.TMALL_COUNT_BOLT_ID:
                TMOrderSet.addAll((ArrayList<Long>) input.getValue(0));
                break;
            default:
                break;
        }
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
         // todo 改成异步 -> collector 机制不确定
        if (flag == 1) {
            fixedThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    Object[] obj = tbWaittingEmitList.poll();
                    while (obj!=null) {
                        collector.emit(RaceConfig.TAOBAO_PERSIST_STREAM_ID, new Values(obj[0],obj[1]));
                        obj = tbWaittingEmitList.poll();
                    }
                }
            });
        } else {
            fixedThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    Object[] obj = tmWaittingEmitList.poll();
                    while (obj != null) {
                        collector.emit(RaceConfig.TMALL_PERSIST_STREAM_ID, new Values(obj[0],obj[1]));
                        obj = tmWaittingEmitList.poll();
                    }
                }
            });
        }
    }
}
