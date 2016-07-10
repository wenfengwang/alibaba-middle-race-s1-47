package com.alibaba.middleware.race.jstorm.blot;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.model.AmountProcess;
import com.alibaba.middleware.race.test.AnalyseThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by wangwenfeng on 5/27/16.
 * 持久化数据模块
 */

public class PersistBolt implements IBasicBolt, Serializable {
    private static Logger LOG = LoggerFactory.getLogger(PersistBolt.class);

    private final String prefix;
    private volatile long currentTimeStamp;
    private volatile boolean endFlag = false;
    private double sumAmount;
    private TopologyContext context;

    private transient AmountProcess amountProcess;

    public PersistBolt() {
        prefix = null;
    }
    public PersistBolt(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.context = context;
        this.currentTimeStamp = 0;
        sumAmount = 0;
        amountProcess = new AmountProcess();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            long minuteTimeStamp = (long) input.getValue(0);
            double amount = (double) input.getValue(1);
            if (endFlag) {
                amountProcess.updateAmount(minuteTimeStamp,amount,prefix);
                amountProcess.writeTair(minuteTimeStamp);
                return;
            }

            // 因外在大概率上消息顺序是有序的,所以仅当时间戳的值改变后我们对当前的值进行持久化操作
            // 每次应该都是写current的时间戳
            if (currentTimeStamp != minuteTimeStamp) {
                if (currentTimeStamp == 0 ) { // 初始化
                    amountProcess.updateAmount(minuteTimeStamp,amount,prefix);
                    currentTimeStamp = minuteTimeStamp;
                    return;
                } else if (minuteTimeStamp == -1 && amount == -1) {
                    if (!RaceConfig.ONLINE) {
                        if (context.getThisComponentId().equals(RaceConfig.TAOBAO_PERSIST_BOLT_ID)) {
                            new Thread(new AnalyseThread(RaceConfig.TB_LOG_PATH,1)).start();
                        } else {
                            new Thread(new AnalyseThread(RaceConfig.TM_LOG_PATH,2)).start();
                        }
                    }
                    endFlag = true;
                    amountProcess.updateAmount(currentTimeStamp,sumAmount,prefix);
                    amountProcess.writeTair(currentTimeStamp);
                    sumAmount = 0;
                    return;
                } else {
                    amountProcess.updateAmount(currentTimeStamp,sumAmount,prefix);
                    amountProcess.writeTair(currentTimeStamp);
                    currentTimeStamp = minuteTimeStamp;
                    sumAmount = 0;
                }
            }
            sumAmount += amount;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup() {
//        Set<Map.Entry<Long, Double>> entrySet = amountMap.entrySet();
//        for(Map.Entry<Long, Double> entry : entrySet) {
//            LOG.info("***** " + prefix+entry.getKey()+": " + entry.getValue()+ " *****");
//        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
