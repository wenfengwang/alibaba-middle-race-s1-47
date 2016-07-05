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
import com.alibaba.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by wangwenfeng on 5/27/16.
 */
// TODO 非事务环境中，尽量使用IBasicBolt -> https://github.com/alibaba/jstorm/wiki/%E5%BC%80%E5%8F%91%E7%BB%8F%E9%AA%8C%E6%80%BB%E7%BB%93
public class CountBolt implements IBasicBolt, Serializable {
    private static Logger LOG = LoggerFactory.getLogger(CountBolt.class);

    private static ConcurrentHashMap<Long,HashSet<Long>> orderMap;
    private SimpleDateFormat sdf;
    private final static AtomicInteger[] atomicIntegers = new AtomicInteger[]{new AtomicInteger(0),new AtomicInteger(0)};
    private final static Object lockObj = new Object();
    private long currentTime;
    private double amount;
    private AtomicBoolean endFlag = new AtomicBoolean(false);

    private final boolean checkDuplicated = RaceConfig.CHECK_ORDER_DUPLICATED;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        synchronized (lockObj) {
            if (orderMap == null) {
               orderMap = new ConcurrentHashMap<>();
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
            String topic = mqTuple.getMq().getTopic();
            byte[] body;
            MessageExt msg;// TODO 测试下在for循环内部定义和外部定义的性能差别
            int size = list.size();

            // TODO 肯定是一个Tuple的 -> 肯定来自一个topic
            for (int i = 0; i < size; i++) {
                msg = list.get(i);
                body = msg.getBody();
                if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                    endFlag.set(true);
                    collector.emit(new Values(currentTime,amount));
                    amount = 0;
                    collector.emit(new Values("end","end"));
                    return;
                }

                OrderMessage order = RaceUtils.readKryoObject(OrderMessage.class, body);

                long timeStamp = sdf.parse(sdf.format(new Date(order.getCreateTime()))).getTime()/1000;

                if (currentTime != timeStamp) {
                    if (currentTime != 0) {
                        collector.emit(new Values(currentTime,amount));
                        amount = 0;
                    }
                    currentTime = timeStamp;
                }

                if (checkDuplicated) {
                    HashSet<Long> orderIdSet = orderMap.get(timeStamp);
                    if (orderIdSet == null) { // 创建orderIdSet
                        synchronized (lockObj) { // TODO 测试多个bolt加锁性能和单个bolt不加锁性能差别
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
                    if (RaceConfig.MqTaobaoTradeTopic.equals(topic)) {
                        atomicIntegers[0].addAndGet(1);
                        LOG.info("***** Taobao Message Numbers: " + atomicIntegers[0].get() + " *****");
                    } else if (RaceConfig.MqTmallTradeTopic.equals(topic)){
                        atomicIntegers[1].addAndGet(1);
                        LOG.info("***** Tmall Message Numbers: " + atomicIntegers[1].get() + " *****");
                    }
                }
                amount += order.getTotalPrice();
                if (endFlag.get()) {
                    collector.emit(new Values(currentTime,amount));
                    amount = 0;
                }
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
        declarer.declare(new Fields("createTime","totalPrice"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
