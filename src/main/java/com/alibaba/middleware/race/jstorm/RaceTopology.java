package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
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
        HashMap tpConf = new HashMap();
        tpConf.put(Config.TOPOLOGY_WORKERS, 4);

        try {
//            StormSubmitter.submitTopology(RaceConfig.JstormTopologyName, tpConf, setBuilderWithPush);
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology(RaceConfig.JstormTopologyName, tpConf, setBuilderWithPush());
            Thread.sleep(1000000);
            localCluster.shutdown();
            LOG.info("Topology submitted!!!!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static StormTopology setBuilderWithPull() {
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

        builder.setSpout("PaymentSpout",new RaceSpoutPull(RaceConfig.MqPayTopic), spout_Parallelism_hint);
        builder.setBolt("splitPayment", new RatioBolt(), bolt_Parallelism_hint).shuffleGrouping("PaymentSpout");
        builder.setBolt("CountPayment", new RatioCount(),_bolt_Parallelism_hint).shuffleGrouping("splitPayment");
        builder.setBolt("PersistRatio", new PersistRatio(),_bolt_Parallelism_hint).shuffleGrouping("CountPayment");
        return builder.createTopology();
    }

    public static StormTopology setBuilderWithPush() {
        HashMap<Object, Object> publicSpoutConfig = new HashMap();
        publicSpoutConfig.put(SpoutConfig.META_CONSUMER_GROUP, RaceConfig.MqConsumerGroup);
        publicSpoutConfig.put(SpoutConfig.META_NAMESERVER,RaceConfig.MQNameServerAddr);

        int spout_Parallelism_hint = 2;
        int bolt_Parallelism_hint = 2;
        int _bolt_Parallelism_hint = 1;
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(RaceConfig.SPOUT_NAME,new RaceSpout(publicSpoutConfig), spout_Parallelism_hint);

        builder.setBolt("CountTaobao", new CountBolt(), bolt_Parallelism_hint).shuffleGrouping(RaceConfig.SPOUT_NAME,RaceConfig.TAOBAO_STREAM_ID);
        builder.setBolt("PerisistTaobao", new PersistBolt(RaceConfig.prex_taobao),_bolt_Parallelism_hint).shuffleGrouping("CountTaobao");

        builder.setBolt("CountTmall", new CountBolt(), bolt_Parallelism_hint).shuffleGrouping(RaceConfig.SPOUT_NAME,RaceConfig.TMALL_STREAM_ID);
        builder.setBolt("PerisistTmall", new PersistBolt(RaceConfig.prex_tmall),_bolt_Parallelism_hint).shuffleGrouping("CountTmall");

        builder.setBolt("splitPayment", new RatioBolt(), bolt_Parallelism_hint).shuffleGrouping(RaceConfig.SPOUT_NAME,RaceConfig.PAYMENT_STREAM_ID);
        builder.setBolt("CountPayment", new RatioCount(),_bolt_Parallelism_hint).shuffleGrouping("splitPayment");
        builder.setBolt("PersistRatio", new PersistRatio(),_bolt_Parallelism_hint).shuffleGrouping("CountPayment");
        return builder.createTopology();
    }
}
