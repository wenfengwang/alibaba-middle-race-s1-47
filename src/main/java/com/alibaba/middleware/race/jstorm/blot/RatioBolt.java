package com.alibaba.middleware.race.jstorm.blot;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import clojure.lang.IFn;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.jstorm.spout.MqTuple;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.codahale.metrics.RatioGauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by wangwenfeng on 6/29/16.
 */
public class RatioBolt implements IBasicBolt, Serializable {
    private static Logger LOG = LoggerFactory.getLogger(RatioBolt.class);

    private static ConcurrentHashMap<Long,HashSet<Long>> paymentMap;
    private final static AtomicInteger atomicInteger = new AtomicInteger(0);

    private SimpleDateFormat sdf;
    private final static Object lockObj = new Object();
    private long currentTime;
    private double amount;

    private final boolean checkDuplicated = RaceConfig.CHECK_PAYMENT_DUPLICATED;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        synchronized (lockObj) {
            if (paymentMap == null) {
                paymentMap = new ConcurrentHashMap<>();
            }
        }
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        currentTime = 0;
        amount = 0;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            MqTuple mqTuple = (MqTuple) input.getValue(0);
            List<MessageExt> list = mqTuple.getMsgList();
            byte[] body;
            MessageExt msg;
            int size = list.size();

            HashMap<Long,double[]> emitTuple = new HashMap<>();
            for (int i = 0; i < size; i++) {
                msg = list.get(i);
                body = msg.getBody();
                if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                    emitTuple.put(-1l,new double[]{-1,-1});
                    continue;
                }
                atomicInteger.addAndGet(1);
                PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);

                long timeStamp = sdf.parse(sdf.format(new Date(paymentMessage.getCreateTime()))).getTime()/1000;
                double[] node = emitTuple.get(timeStamp); // 0 PC 1 MOBILE
                if (node == null) {
                    node = new double[]{0,0};
                }

//                if (checkDuplicated) {
//                    HashSet<Long> orderIdSet = paymentMap.get(timeStamp);
//                    if (orderIdSet == null) { // 创建orderIdSet
//                        synchronized (lockObj) { // TODO 测试多个bolt加锁性能和单个bolt不加锁性能差别
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
            LOG.info("***** Payment Message Numbers: " + atomicInteger.get() + " *****");
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
