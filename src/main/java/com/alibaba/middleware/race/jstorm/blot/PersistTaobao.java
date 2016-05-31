package com.alibaba.middleware.race.jstorm.blot;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wangwenfeng on 5/27/16.
 * 持久化数据模块
 */
public class PersistTaobao implements IRichBolt, Serializable {
    OutputCollector collector;
    Map<Long, Double> counts = new ConcurrentHashMap<Long, Double>();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    TairOperatorImpl tairOperator;

    public PersistTaobao() {
        tairOperator = new TairOperatorImpl();
    }
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        ArrayList<Object[]> list = (ArrayList<Object[]>) input.getValue(0);
        int size = list.size();
        for (int i = 0; i < size; i++) {
            long timestamp = (Long) list.get(i)[0];
            long minuteTimeStamp = 0;
            try {
                // TODO 这个地方的时间戳解析性能测试
                minuteTimeStamp = sdf.parse(sdf.format(new Date(timestamp))).getTime();
            } catch (ParseException e) {
                e.printStackTrace();
            }
            // TODO 这个地方的加法操作是安全的吗
            double totalPrice = (Double) list.get(i)[1] + counts.get(minuteTimeStamp);
            counts.put(minuteTimeStamp,totalPrice);
        }
        //定时写数据到Tair
//        tairOperator.write(new String(RaceConfig.prex_taobao+minuteTimeStamp), totalPrice);
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

//    public static void main(String[] args) {
//        PersistTaobao persistTaobao = new PersistTaobao();
//        Date date = new Date();
//        System.out.println("current time: " + date);
//        System.out.println("current timestamp: " + date.getTime());
//        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm");
//        String sd = sdf.format(date.getTime());
//
//        Date time = null;
//        try {
//            time = sdf.parse(sd);
//            System.out.println("parsed time: " + time);
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//        System.out.println("parsed timestamp: " + time.getTime());
//        System.out.println("distance: " + (date.getTime() - time.getTime()));
//    }
}
