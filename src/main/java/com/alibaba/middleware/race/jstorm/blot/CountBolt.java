package com.alibaba.middleware.race.jstorm.blot;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.jstorm.spout.MqTuple;
import com.alibaba.middleware.race.model.OrderMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by wangwenfeng on 5/27/16.
 */
public class CountBolt implements IBasicBolt, Serializable {
    private static Logger LOG = LoggerFactory.getLogger(CountBolt.class);

    private static ConcurrentHashMap<Long,HashSet<Long>> orderMap;
    private final static AtomicInteger[] atomicIntegers = new AtomicInteger[]{new AtomicInteger(0),new AtomicInteger(0)};
    private final static Object lockObj = new Object();

    private final boolean checkDuplicated = RaceConfig.CHECK_ORDER_DUPLICATED;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        synchronized (lockObj) {
            if (orderMap == null) {
               orderMap = new ConcurrentHashMap<>();
            }
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            MqTuple mqTuple = (MqTuple) input.getValue(0);
            List<byte[]> list = mqTuple.getMsgList();
            String topic = mqTuple.getTopic();
            byte[] body;
            int size = list.size();

            HashMap<Long, Double> emitTuple = new HashMap<>();
            for (int i = 0; i < size; i++) {
                body = list.get(i);
                if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                    emitTuple.put(-1l,-1.0);
                    continue;
                }

                OrderMessage order = RaceUtils.readKryoObject(OrderMessage.class, body);
                long timeStamp = RaceUtils.toMinuteTimeStamp(order.getCreateTime());
                Double amount = emitTuple.get(timeStamp);
                if (amount == null) {
                    amount = 0.0;
                }

                if (checkDuplicated) {
                    HashSet<Long> orderIdSet = orderMap.get(timeStamp);
                    if (orderIdSet == null) { // 创建orderIdSet
                        synchronized (lockObj) {
                            orderIdSet = orderMap.get(timeStamp);
                            if (orderIdSet == null) {
                                orderIdSet = new HashSet<>();
                                orderMap.put(timeStamp, orderIdSet);
                            }
                        }
                    }

                    if (orderIdSet.contains(timeStamp)) {
                        continue;
                    } else {
                        synchronized (orderIdSet) {
                            orderIdSet.add(order.getOrderId());
                        }
                    }
                }
                if (RaceConfig.MqTaobaoTradeTopic.equals(topic)) {
                    atomicIntegers[0].addAndGet(1);
                } else if (RaceConfig.MqTmallTradeTopic.equals(topic)){
                    atomicIntegers[1].addAndGet(1);
                }
                amount += order.getTotalPrice();
                emitTuple.put(timeStamp,amount);
            }
            if (RaceConfig.MqTaobaoTradeTopic.equals(topic)) {
                LOG.warn("Taobao Message Numbers: " + atomicIntegers[0].get() + ", Tmall Message Numbers: " + atomicIntegers[1].get());
            }
            collector.emit(new Values(emitTuple));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("emitTuple"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
