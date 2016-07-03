package com.alibaba.middleware.race.jstorm.blot;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
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
 * Created by wangwenfeng on 5/27/16.
 */
// TODO 非事务环境中，尽量使用IBasicBolt -> https://github.com/alibaba/jstorm/wiki/%E5%BC%80%E5%8F%91%E7%BB%8F%E9%AA%8C%E6%80%BB%E7%BB%93
public class CountBolt implements IBasicBolt, Serializable {
    private static Logger LOG = LoggerFactory.getLogger(CountBolt.class);
    private int platForm = -1;
    private static final String[] platFormStr = new String[]{"Tmall","Taobao"};
    // 淘宝订单 天猫订单 付款订单
    private static AtomicInteger[] count = new AtomicInteger[]{new AtomicInteger(0),new AtomicInteger(0)};
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
                if (platForm == -1) {
                    String saler = order.getSalerId();
                    if (saler.startsWith("tm_")) {
                        platForm = 0;
                    }else {
                        platForm = 1;
                    }
                }
                count[platForm].addAndGet(1);
                // TODO 每条emit的效率和放到一起直接emit哪个高? 另外, ack的时间会比较高
                collector.emit(new Values(order.getCreateTime(), order.getTotalPrice()));
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
