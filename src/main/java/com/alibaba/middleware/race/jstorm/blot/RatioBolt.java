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
import com.alibaba.middleware.race.model.PaymentMessage;
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
 * Created by wangwenfeng on 6/29/16.
 */
public class RatioBolt implements IBasicBolt, Serializable {
    private static Logger LOG = LoggerFactory.getLogger(RatioBolt.class);

    private static ConcurrentHashMap<Long,HashSet<Long>> paymentMap;
    private final static AtomicInteger atomicInteger = new AtomicInteger(0);

    private final static Object lockObj = new Object();

    private final boolean checkDuplicated = RaceConfig.CHECK_PAYMENT_DUPLICATED;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        synchronized (lockObj) {
            if (paymentMap == null) {
                paymentMap = new ConcurrentHashMap<>();
            }
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

        try {
            MqTuple mqTuple = (MqTuple) input.getValue(0);
            List<byte[]> list = mqTuple.getMsgList();
            byte[] body;
            int size = list.size();

            HashMap<Long,double[]> emitTuple = new HashMap<>();
            for (int i = 0; i < size; i++) {
                body = list.get(i);
                if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                    emitTuple.put(-1l,new double[]{-1,-1});
                    continue;
                }
                atomicInteger.addAndGet(1);
                PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
                long timeStamp = RaceUtils.toMinuteTimeStamp(paymentMessage.getCreateTime());
                double[] node = emitTuple.get(timeStamp); // 0 PC 1 MOBILE
                if (node == null) {
                    node = new double[]{0,0};
                }

//                if (checkDuplicated) {
//                    HashSet<Long> orderIdSet = paymentMap.get(timeStamp);
//                    if (orderIdSet == null) { // 创建orderIdSet
//                        synchronized (lockObj) {
//                            orderIdSet = paymentMap.get(timeStamp);
//                            if (orderIdSet == null) {
//                                orderIdSet = new HashSet<>();
//                                paymentMap.put(timeStamp, orderIdSet);
//                            }
//                        }
//                    }
//
//                    if (orderIdSet.contains(timeStamp)) {
//                        continue;
//                    } else {
//                        synchronized (orderIdSet) {
//                            orderIdSet.add(paymentMessage.getOrderId());
//                        }
//                    }
//                }
                node[paymentMessage.getPayPlatform()] += paymentMessage.getPayAmount();
                emitTuple.put(timeStamp,node);
            }
            LOG.warn("***** Payment Message Numbers: " + atomicInteger.get() + " *****");
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
        declarer.declare(new Fields("payInfoHash"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
