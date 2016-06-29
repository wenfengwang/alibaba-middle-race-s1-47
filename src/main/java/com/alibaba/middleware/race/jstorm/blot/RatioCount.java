package com.alibaba.middleware.race.jstorm.blot;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
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
 * Created by wangwenfeng on 6/29/16.
 */
public class RatioCount implements IRichBolt, Serializable {
    private OutputCollector collector;
    private static Logger LOG = LoggerFactory.getLogger(RatioCount.class);

    private SimpleDateFormat sdf;
    private Map<String,double[]> messageMap;
    TairOperatorImpl tairOperator;

    private volatile boolean changed = false;

    private String concurrentTimeStamp;
    private String oldTimeStamp;
    private long concurrentPCAmount;
    private long concurrentMobileAmount;

    private String prefix;

    public RatioCount() {}

    public RatioCount(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        messageMap = new ConcurrentHashMap<String, double[]>();

        ArrayList<String> list = new ArrayList<String>();
        list.add("192.168.1.161:5198");
        tairOperator = new TairOperatorImpl(list);

        this.concurrentTimeStamp = "";
        this.oldTimeStamp = "";
        this.concurrentPCAmount = 0;
        this.concurrentMobileAmount = 0;
    }

    @Override
    public void execute(Tuple input) {
        String minuteTimeStamp;
        short payPlatform = (Short) input.getValue(1);

        try {
            minuteTimeStamp = String.valueOf(sdf.parse(sdf.format(new Date((Long) input.getValue(0)))).getTime()).substring(0,10);
            if (!minuteTimeStamp.equals(concurrentTimeStamp)) {
                oldTimeStamp = concurrentTimeStamp;
                concurrentTimeStamp = minuteTimeStamp;
                changed = true;
            }

            double[] amountArr =  messageMap.get(minuteTimeStamp);
            if (amountArr == null) {
                amountArr = new double[]{0,0};
            }
            amountArr[payPlatform] += (Double)input.getValue(2);
            messageMap.put(minuteTimeStamp,amountArr);
            if (changed) {
                double[] lastMessage = messageMap.get(oldTimeStamp);
                tairOperator.write(prefix+oldTimeStamp, lastMessage[1]/lastMessage[0]);
                collector.ack(input);
                changed = false;
            }
        } catch (Exception e) {
            if ("".equals(input.getValue(0)) && "".equals(input.getValue(1)) && "".equals(input.getValue(2))) {
                double[] lastMessage = messageMap.get(concurrentTimeStamp);
                tairOperator.write(prefix+concurrentTimeStamp, lastMessage[1]/lastMessage[0]); //TODO 两位小数
                System.out.println(concurrentTimeStamp);
                System.out.println(messageMap.get(concurrentTimeStamp));
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
