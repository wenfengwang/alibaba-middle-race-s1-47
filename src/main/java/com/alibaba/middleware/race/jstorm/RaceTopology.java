package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.blot.*;
import com.alibaba.middleware.race.jstorm.spout.RaceSpout;
import com.alibaba.middleware.race.jstorm.spout.RaceSpoutPull;
import com.alibaba.middleware.race.jstorm.spout.SpoutConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * 选手拓扑入口类，我们定义必须是com.alibaba.middleware.race.jstorm.RaceTopology
 * 因为我们后台对选手的git进行下载打包，拓扑运行的入口类默认是com.alibaba.middleware.race.jstorm.RaceTopology；
 * 所以这个主类路径一定要正确
 */
public class RaceTopology {

    private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);

    public static void main(String[] args) throws Exception {

        // Toplology Configuration
        HashMap tpConf = new HashMap();
        tpConf.put(Config.TOPOLOGY_WORKERS, 4);
        tpConf.put(Config.TOPOLOGY_ACKER_EXECUTORS,1);  //等价于 Config.setNumAckers(tpConf,1);

        // Spout's public configuration
        HashMap<Object, Object> publicSpoutConfig = new HashMap();
        publicSpoutConfig.put(SpoutConfig.META_CONSUMER_GROUP, RaceConfig.MqConsumerGroup);
//        publicSpoutConfig.put(SpoutConfig.META_NAMESERVER,RaceConfig.MQNameServerAddr);

        // Spout's Topic configuration
        HashMap<Object,Object> confTaobao = new HashMap(publicSpoutConfig);
        confTaobao.put(SpoutConfig.META_TOPIC,RaceConfig.MqTaobaoTradeTopic);

        HashMap<Object,Object> confTmall = new HashMap(publicSpoutConfig);
        confTmall.put(SpoutConfig.META_TOPIC,RaceConfig.MqTmallTradeTopic);

        HashMap<Object,Object> confPayment= new HashMap(publicSpoutConfig);
        confPayment.put(SpoutConfig.META_TOPIC,RaceConfig.MqPayTopic);

        // TODO 设计acker https://github.com/alibaba/jstorm/wiki/Ack-%E6%9C%BA%E5%88%B6 -> msgId具体是?
        int spout_Parallelism_hint = 1;
        int bolt_Parallelism_hint = 2;
        int _bolt_Parallelism_hint = 1;
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("TaobaoSpout",new RaceSpoutPull(RaceConfig.MqTaobaoTradeTopic), spout_Parallelism_hint);
        builder.setBolt("CountTaobao", new CountBolt(), bolt_Parallelism_hint).shuffleGrouping("TaobaoSpout");
        builder.setBolt("PerisistTaobao", new PersistBolt(RaceConfig.prex_taobao),_bolt_Parallelism_hint).shuffleGrouping("CountTaobao");

        builder.setSpout("TmallSpout",new RaceSpoutPull(RaceConfig.MqTmallTradeTopic), spout_Parallelism_hint);
        builder.setBolt("CountTmall", new CountBolt(), bolt_Parallelism_hint).shuffleGrouping("TmallSpout");
        builder.setBolt("PerisistTmall", new PersistBolt(RaceConfig.prex_tmall),_bolt_Parallelism_hint).shuffleGrouping("CountTmall");

//        builder.setSpout("PaymentSpout",new RaceSpout(confPayment), spout_Parallelism_hint);
        builder.setSpout("PaymentSpout",new RaceSpoutPull(RaceConfig.MqPayTopic), spout_Parallelism_hint);
        builder.setBolt("splitPayment", new RatioBolt(), bolt_Parallelism_hint).shuffleGrouping("PaymentSpout");
        builder.setBolt("CountPayment", new RatioCount(),_bolt_Parallelism_hint).shuffleGrouping("splitPayment");
        builder.setBolt("PersistRatio", new PersistRatio(),_bolt_Parallelism_hint).shuffleGrouping("CountPayment");

        try {
            StormSubmitter.submitTopology(RaceConfig.JstormTopologyName, tpConf, builder.createTopology());
//            LocalCluster localCluster = new LocalCluster();
//            localCluster.submitTopology(RaceConfig.JstormTopologyName, tpConf, builder.createTopology());
//            Thread.sleep(1000000);
//            localCluster.shutdown();
//            LOG.info("Topology submitted!!!!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
