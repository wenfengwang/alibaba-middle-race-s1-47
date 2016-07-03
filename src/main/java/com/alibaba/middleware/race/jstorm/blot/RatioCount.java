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
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.Ratio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.twitter.chill.config.ReflectingInstantiator.prefix;

/**
 * Created by wangwenfeng on 6/29/16.
 */
public class RatioCount implements IBasicBolt, Serializable {
    private static Logger LOG = LoggerFactory.getLogger(RatioCount.class);
    private long conCurrentTime;
    private double[] sumAmount;
    private boolean endFlag = false;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        conCurrentTime = 0;
        sumAmount = new double[]{0,0};
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        // 仅作切分
        HashMap<Long, double[]> tuple = (HashMap<Long, double[]>) input.getValue(0);

        Set<Map.Entry<Long,double[]>> entrySet = tuple.entrySet();
        for (Map.Entry<Long, double[]> entry : entrySet) {
            long minuteTimeStamp = entry.getKey();
            double[] amount = entry.getValue();
            if (minuteTimeStamp == -1 && amount[0] == -1 && amount[1] == -1 ) {
                collector.emit(new Values(-1l,new double[]{-1,-1}));
                endFlag = true;
                return;
            }
            if (minuteTimeStamp != conCurrentTime) {
                collector.emit(new Values(conCurrentTime, sumAmount));
                conCurrentTime = minuteTimeStamp;
                sumAmount[0] = sumAmount[1] = 0;
            }
            sumAmount[0] += amount[0];
            sumAmount[1] += amount[1];
            if (endFlag) {
                collector.emit(new Values(conCurrentTime, sumAmount));
                sumAmount[0] = sumAmount[1] = 0;
            }
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timeStamp","sumAmount"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
