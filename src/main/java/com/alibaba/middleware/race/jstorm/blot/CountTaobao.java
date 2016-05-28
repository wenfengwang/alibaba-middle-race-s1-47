package com.alibaba.middleware.race.jstorm.blot;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by wangwenfeng on 5/27/16.
 */
public class CountTaobao implements IRichBolt, Serializable {
    OutputCollector collector;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        System.out.println("CountTaobao started");
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        // TODO 是不是这个作用?
        List list = input.getValues();
        byte[] body;
        MessageExt msg;// TODO 测试下在for循环内部定义和外部定义的性能差别
        int size = list.size();
        for (int i = 0; i < size; i++) {
            List<Object> emitList = new ArrayList<Object>();// TODO 测试下在外面定义和里面定义的性能
            msg = (MessageExt) list.get(i);
            body = msg.getBody();
            OrderMessage order = RaceUtils.readKryoObject(OrderMessage.class, body);
            emitList.add(order.getCreateTime());
            emitList.add(order.getTotalPrice());
            collector.emit(emitList);
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
