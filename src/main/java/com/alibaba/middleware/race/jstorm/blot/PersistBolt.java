package com.alibaba.middleware.race.jstorm.blot;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wangwenfeng on 5/27/16.
 * 持久化数据模块
 */

// TODO 各个Topic的持久化bolt应该仅设置为一个, 如果设置为多个时候可能会导致数据分流,而使得消息缺失, 必须去从Tair中读取.而且多个持久化的
// TODO bolt,会存在若干线程不安全的情况

public class PersistBolt implements IBasicBolt, Serializable {
    private static Logger LOG = LoggerFactory.getLogger(PersistBolt.class);

    private Map<String, Double> counts;
    private SimpleDateFormat sdf;
    TairOperatorImpl tairOperator;

    private volatile String concurrentTimeStamp;
    private volatile String oldTimeStamp;
    private volatile boolean changed = false;
    private String prefix;

    public PersistBolt() {}

    public PersistBolt(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        counts = new ConcurrentHashMap<String, Double>();
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");

        this.concurrentTimeStamp = "";
        this.oldTimeStamp = "";
        ArrayList<String> list = new ArrayList<String>();
        list.add("192.168.1.161:5198");
        tairOperator = new TairOperatorImpl(list);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String minuteTimeStamp;
        try {
            // 因外在大概率上消息顺序是有序的,所以仅当时间戳的值改变后我们对当前的值进行持久化操作
            minuteTimeStamp = String.valueOf(sdf.parse(sdf.format(new Date((Long) input.getValue(0)))).getTime()).substring(0,10);
            if (!minuteTimeStamp.equals(concurrentTimeStamp)) {
                concurrentTimeStamp = minuteTimeStamp;
                changed = true;
            }

            // 为什么一直会抛出空指针异常 -> 因为当get找不到值的时候，会返回null，而等号的左边是double类型，不接受null，也不
            // 自动转换，所以就抛出了空指针异常。
            // 空指针异常并不是仅仅限于调用者为空，异常的null变量赋值也会导致空指针异常
            String sumPrice =  String.valueOf(counts.get(minuteTimeStamp));//String.valueOf(minuteTimeStamp)
            double totalPrice = (Double)input.getValue(1) + (("null").equals(sumPrice) ? 0.0: Double.valueOf(sumPrice));
            counts.put(String.valueOf(minuteTimeStamp),totalPrice);
            LOG.info(String.valueOf(sumPrice));
            if (changed) {
                // TODO 这个地方存在线程不安全的可能吗? -> 单个bolt线程安全, 多个不安全
                tairOperator.write(prefix+concurrentTimeStamp, totalPrice);
                changed = false;
            }
        } catch (Exception e) {
            // 结束时将当前结果写入到tair
            if ("".equals(input.getValue(0)) && "".equals(input.getValue(1))) {
                tairOperator.write(prefix+concurrentTimeStamp, counts.get(concurrentTimeStamp));
                System.out.println(concurrentTimeStamp);
                System.out.println(counts.get(concurrentTimeStamp));
            }
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
