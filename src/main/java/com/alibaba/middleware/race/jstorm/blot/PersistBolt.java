package com.alibaba.middleware.race.jstorm.blot;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.test.AnalyseThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wangwenfeng on 5/27/16.
 * 持久化数据模块
 */

// TODO 各个Topic的持久化bolt应该仅设置为一个, 如果设置为多个时候可能会导致数据分流,而使得消息缺失, 必须去从Tair中读取.而且多个持久化的
// TODO bolt,会存在若干线程不安全的情况

public class PersistBolt implements IBasicBolt, Serializable {
    private static Logger LOG = LoggerFactory.getLogger(PersistBolt.class);

    private Map<Long, Double> amountMap;
    TairOperatorImpl tairOperator;

    private volatile long currentTimeStamp;
    private static volatile boolean endFlag = false;
    private double amount;
    private String prefix;
    private TopologyContext context;

    public PersistBolt() {}
    public PersistBolt(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.context = context;
        amountMap = new ConcurrentHashMap<>();
        this.currentTimeStamp = 0;
        amount = 0;
        tairOperator = new TairOperatorImpl(RaceConfig.TairServerAddr, RaceConfig.TairNamespace);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            // TODO 最后的持久化有问题  天猫的生成数据是不是有问题
            HashMap<Long, Double> tuple = (HashMap<Long, Double>) input.getValue(0);
            Set<Map.Entry<Long, Double>> entrySet = tuple.entrySet();
            for (Map.Entry<Long, Double> entry : entrySet) {
                long minuteTimeStamp = entry.getKey();
                double price = entry.getValue();
                if (endFlag) {
                    // 这里面应该不会存在极端情况吧?
                    Double totalPrice = amountMap.get(minuteTimeStamp); // 收到endflag的时候current相关的已被处理。
                    if (totalPrice == null) {
                        totalPrice = 0.0;
                    }
                    totalPrice += price;
                    amountMap.put(minuteTimeStamp,totalPrice);
                    tairOperator.write(prefix+minuteTimeStamp, totalPrice);
                    continue;
                }

                // 因外在大概率上消息顺序是有序的,所以仅当时间戳的值改变后我们对当前的值进行持久化操作
                // 每次应该都是写current的时间戳
                if (currentTimeStamp != minuteTimeStamp) {
                    double totalPrice = amountMap.get(currentTimeStamp) == null ? amount : amountMap.get(currentTimeStamp) + amount;
                    if (currentTimeStamp == 0 ) { // 初始化
                        currentTimeStamp = minuteTimeStamp;
                        amountMap.put(currentTimeStamp, price);
                        continue;
                    } else if (minuteTimeStamp == -1 && price == -1) {
                        if (!RaceConfig.ONLINE) {
                            if (context.getThisComponentId().equals(RaceConfig.TAOBAO_PERSIST_BOLT_ID)) {
                                new Thread(new AnalyseThread(RaceConfig.TB_LOG_PATH,1)).start();
                            } else {
                                new Thread(new AnalyseThread(RaceConfig.TM_LOG_PATH,2)).start();
                            }
                        }
                        endFlag = true;
                        amountMap.put(currentTimeStamp, totalPrice);
                        tairOperator.write(prefix+currentTimeStamp, totalPrice);
                        amount = 0;
                        continue;
                    } else {
                        amountMap.put(currentTimeStamp, totalPrice);
                        tairOperator.write(prefix+currentTimeStamp, totalPrice);
                        currentTimeStamp = minuteTimeStamp;
                        amount = 0;
                    }
                }
                amount += price;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
