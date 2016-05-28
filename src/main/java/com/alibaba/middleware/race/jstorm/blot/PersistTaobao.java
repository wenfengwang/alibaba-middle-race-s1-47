package com.alibaba.middleware.race.jstorm.blot;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wangwenfeng on 5/27/16.
 * 持久化数据模块
 */
public class PersistTaobao implements IRichBolt {
    OutputCollector collector;
    Map<Long, Double> counts = new ConcurrentHashMap<Long, Double>();
    TairOperatorImpl tairOperator;

    public PersistTaobao() {
        tairOperator = new TairOperatorImpl();
    }
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        System.out.println("PersistTaobao started");
    }

    @Override
    public void execute(Tuple input) { // TODO 完善逻辑
        List<Object> list = input.getValues();
        long minTimeStamp = formatCreateTime((Long) list.get(0));

        // TODO 这个地方的加法操作是安全的吗
        double totalPrice = (Double) list.get(1) + counts.get(minTimeStamp);;
        counts.put(minTimeStamp,totalPrice);
        System.out.println("**********");
        //定时写数据到Tair
        tairOperator.write(new String(RaceConfig.prex_taobao+minTimeStamp),totalPrice);
    }

    @Override
    public void cleanup() {

    }

    public long formatCreateTime(long createTime) {
        // TODO 将createTime转化为整分时间戳
        return 0l;
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
