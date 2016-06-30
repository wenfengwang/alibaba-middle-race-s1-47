package com.alibaba.middleware.race.jstorm.blot;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.jstorm.spout.MqTuple;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by sxian.wang on 2016/6/30.
 */
public class DistributionBolt implements IBasicBolt, Serializable {
    private static Logger LOG = LoggerFactory.getLogger(DistributionBolt.class);
    // 淘宝订单 天猫订单 付款订单
    private AtomicInteger[] atomCount = new AtomicInteger[]{new AtomicInteger(0),new AtomicInteger(0),new AtomicInteger(0)};

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            MqTuple mqTuple = (MqTuple) input.getValue(0);
            List<MessageExt> list = mqTuple.getMsgList();
            byte[] body;
            MessageExt msg;// TODO 测试下在for循环内部定义和外部定义的性能差别
            int size = list.size();
            long start_time = System.currentTimeMillis();
            for (int i = 0; i < size; i++) {
                msg = list.get(i);
                body = msg.getBody();
                if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                    collector.emit(new Values("",""));
                    return;
                }
                OrderMessage order = RaceUtils.readKryoObject(OrderMessage.class, body);
                LOG.info(order.toString());
                // TODO 每条emit的效率和放到一起直接emit哪个高? 另外, ack的时间会比较高
                collector.emit(new Values(order.getCreateTime(), order.getTotalPrice()));
            }
            LOG.info(String.valueOf(System.currentTimeMillis() - start_time));
//            collector.ack(input);
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
