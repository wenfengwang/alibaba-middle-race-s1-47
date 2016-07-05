package com.alibaba.middleware.race.jstorm.blot;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.test.AnalyseResult;
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

    private Map<String, Double> amountMap;
    TairOperatorImpl tairOperator;

    private volatile String currentTimeStamp;
    private static volatile boolean endFlag = false;
    private double amount;
    private String prefix;
    private AnalyseResult analyseResult;
    private TopologyContext context;
    public PersistBolt() {}

    public PersistBolt(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.context = context;
        // TODO 判断下上下文
        String filePath = "";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
        switch (context.getThisComponentId()) {
            case RaceConfig.TAOBAO_PERSIST_BOLT_ID:
                filePath = RaceConfig.TB_LOG_PATH + sdf.format(new Date(System.currentTimeMillis()));
                break;
            case RaceConfig.TMALL_PERSIST_BOLT_ID:
                filePath = RaceConfig.TM_LOG_PATH + sdf.format(new Date(System.currentTimeMillis()));
                break;
        }
        analyseResult = new AnalyseResult(filePath+".log");
        amountMap = new ConcurrentHashMap<>();
        this.currentTimeStamp = "started";
        amount = 0;
        tairOperator = new TairOperatorImpl(RaceConfig.TairServerAddr, RaceConfig.TairNamespace);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) { // TODO review
        LOG.info("+++++ " + context.getThisComponentId());
        String minuteTimeStamp;
//        context.getZkCluster().
        try {
            // 因外在大概率上消息顺序是有序的,所以仅当时间戳的值改变后我们对当前的值进行持久化操作

            // 为什么一直会抛出空指针异常 -> 因为当get找不到值的时候，会返回null，而等号的左边是double类型，不接受null，也不
            // 自动转换，所以就抛出了空指针异常。
            // 空指针异常并不是仅仅限于调用者为空，异常的null变量赋值也会导致空指针异常
            minuteTimeStamp = String.valueOf(input.getValue(0));
            double price = (double) input.getValue(1);
            if (!currentTimeStamp.equals(minuteTimeStamp)) {
                if (currentTimeStamp.equals("started")) { // 初始化
                    currentTimeStamp = minuteTimeStamp;
                    amount += price;
                } else {
                    double totalPrice = amountMap.get(minuteTimeStamp) + amount;
                    amountMap.put(minuteTimeStamp,totalPrice);
                    tairOperator.write(prefix+currentTimeStamp, totalPrice);
                    LOG.info(prefix+currentTimeStamp + " : " + totalPrice);
                    amount = 0;
                    currentTimeStamp = minuteTimeStamp;
                }
                return;
            }

            amount += price;

            if (endFlag) {
                double totalPrice = amountMap.get(minuteTimeStamp) + amount;
                amountMap.put(minuteTimeStamp,totalPrice);
                amount = 0;
                tairOperator.write(prefix+minuteTimeStamp, totalPrice);
                LOG.info(prefix+minuteTimeStamp + " : " + totalPrice);
            }
        } catch (Exception e) { // 收到结束信号后每次都进行持久化
            if ("end".equals(input.getValue(0)) && "end".equals(input.getValue(1))) {
                endFlag = true;
                try {
                    if (context.getThisComponentId() == "") {
                        analyseResult.analyseTaobao();
                    } else {
                        analyseResult.analyseTmall();
                    }
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
                tairOperator.write(prefix+currentTimeStamp, amountMap.get(currentTimeStamp));
                LOG.info(prefix+currentTimeStamp + " : " +  amountMap.get(currentTimeStamp));
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
