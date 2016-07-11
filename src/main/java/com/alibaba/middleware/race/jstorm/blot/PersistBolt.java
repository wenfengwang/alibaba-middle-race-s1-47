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
import java.util.Map;

/**
 * Created by wangwenfeng on 5/27/16.
 * 持久化数据模块
 */

public class PersistBolt implements IBasicBolt, Serializable {
    private static Logger LOG = LoggerFactory.getLogger(PersistBolt.class);

    private final String prefix;
    private long currentTimeStamp;
    private boolean endFlag = false;
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
                    currentTimeStamp = minuteTimeStamp;
                } else if (minuteTimeStamp == 0 && amount == 0) {
                    if (!RaceConfig.ONLINE) {
                        if (context.getThisComponentId().equals(RaceConfig.TAOBAO_PERSIST_BOLT_ID)) {
                            new Thread(new AnalyseThread(RaceConfig.TB_LOG_PATH,1)).start();
                        } else {
                            new Thread(new AnalyseThread(RaceConfig.TM_LOG_PATH,2)).start();
                        }
                    }
                    endFlag = true;
                    amountProcess.writeTair(currentTimeStamp);
                    return;
                } else {
                    amountProcess.writeTair(currentTimeStamp);
                    currentTimeStamp = minuteTimeStamp;
                }
            }
            amountProcess.updateAmount(minuteTimeStamp,amount,prefix);
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
