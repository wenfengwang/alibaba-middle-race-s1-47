package com.alibaba.middleware.race.jstorm.blot;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by wangwenfeng on 6/29/16.
 */
public class RatioCount implements IBasicBolt, Serializable {
    private static Logger LOG = LoggerFactory.getLogger(RatioCount.class);
    private long currentTimeStamp;
    private double[] sumAmount;
    private boolean endFlag = false;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        currentTimeStamp = 0;
        sumAmount = new double[]{0,0};
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            HashMap<Long, double[]> tuple = (HashMap<Long, double[]>) input.getValue(0);
            Set<Map.Entry<Long,double[]>> entrySet = tuple.entrySet();
            for (Map.Entry<Long, double[]> entry : entrySet) {
                long minuteTimeStamp = entry.getKey();
                double[] platFormPrice = entry.getValue();

                if (endFlag) {
                    collector.emit(new Values(minuteTimeStamp, platFormPrice));
                    continue;
                }

                if (currentTimeStamp != minuteTimeStamp) {
                    if (currentTimeStamp == 0) {
                        currentTimeStamp = minuteTimeStamp;
                        sumAmount = platFormPrice;
                        continue;
                    } else if (minuteTimeStamp == -1 && platFormPrice[0] == -1 && platFormPrice[1] == -1) {
                        collector.emit(new Values(-1l,new double[]{-1,-1}));
                        endFlag = true;
                        continue;
                    } else {
                        collector.emit(new Values(currentTimeStamp, new double[]{sumAmount[0],sumAmount[1]}));
                        currentTimeStamp = minuteTimeStamp;
                        sumAmount[0] = sumAmount[1] = 0;
                    }
                }
                sumAmount[0] += platFormPrice[0];
                sumAmount[1] += platFormPrice[1];
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
        declarer.declare(new Fields("timeStamp","sumAmount"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
