package com.alibaba.middleware.race.jstorm.blot;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.jstorm.spout.MqTuple;
import com.alibaba.middleware.race.jstorm.spout.RaceSpout;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by wangwenfeng on 5/27/16.
 */
public class CountTaobao implements IRichBolt, Serializable {
    private static Logger LOG = LoggerFactory.getLogger(CountTaobao.class);
    OutputCollector collector;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        MqTuple mqTuple = (MqTuple) input.getValue(0);
        List<MessageExt> list = mqTuple.getMsgList();
        byte[] body;
        MessageExt msg;// TODO 测试下在for循环内部定义和外部定义的性能差别
        int size = list.size();
        ArrayList<Object[]> emitList = new ArrayList<Object[]>();
        for (int i = 0; i < size; i++) {
            msg = list.get(i);
            body = msg.getBody();
            OrderMessage order = RaceUtils.readKryoObject(OrderMessage.class, body);
            Object[] objects = new Object[] {order.getCreateTime(), order.getTotalPrice()};
            emitList.add(objects);
            LOG.info(order.toString());
        }
        // TODO 这个地方的需要建个阻塞队列吗
        collector.emit(new Values(emitList));
        LOG.info("************* emited ***************");
        collector.ack(input);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("countTaobao"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
