package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import clojure.lang.Obj;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.blot.CountTaobao;
import com.alibaba.middleware.race.jstorm.blot.PersistTaobao;
import com.alibaba.middleware.race.jstorm.spout.RaceSpout;
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
        tpConf.put(Config.TOPOLOGY_DEBUG, true);
        tpConf.put(Config.TOPOLOGY_ACKER_EXECUTORS,1);  //等价于 Config.setNumAckers(tpConf,1);
        tpConf.put(Config.TOPOLOGY_DEBUG,false);

        // Spout's public configuration
        HashMap<Object, Object> publicSpoutConfig = new HashMap();
        publicSpoutConfig.put(SpoutConfig.META_CONSUMER_GROUP, RaceConfig.MqConsumerGroup);
        publicSpoutConfig.put(SpoutConfig.META_NAMESERVER,RaceConfig.MQNameServerAddr);

        // Spout's Topic configuration
        HashMap<Object,Object> confTaobao = new HashMap(publicSpoutConfig);
        confTaobao.put(SpoutConfig.META_TOPIC,RaceConfig.MqTaobaoTradeTopic);
        HashMap<Object,Object> confTmall = new HashMap(publicSpoutConfig);
        confTmall.put(SpoutConfig.META_TOPIC,RaceConfig.MqTmallTradeTopic);
        HashMap<Object,Object> confPayment= new HashMap(publicSpoutConfig);
        confPayment.put(SpoutConfig.META_TOPIC,RaceConfig.MqPayTopic);

        // TODO 设计acker https://github.com/alibaba/jstorm/wiki/Ack-%E6%9C%BA%E5%88%B6 -> msgId具体是?
        int spout_Parallelism_hint = 1;
        int bolt_Parallelism_hint = 1;
        int _bolt_Parallelism_hint = 1;
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("taobao",new RaceSpout(confTaobao), spout_Parallelism_hint);
        builder.setBolt("countTaobao", new CountTaobao(), bolt_Parallelism_hint).shuffleGrouping("taobao");
        builder.setBolt("perisistTaobao", new PersistTaobao(),_bolt_Parallelism_hint).shuffleGrouping("countTaobao");

//        builder.setSpout("tmall",new RaceSpout(confTmall), spout_Parallelism_hint);
//        builder.setBolt("countTmall", new CountTaobao(), bolt_Parallelism_hint).shuffleGrouping("tmall");

//        builder.setSpout("payment",new RaceSpout(confPayment), spout_Parallelism_hint);
//        builder.setBolt("countPayment", new CountTaobao(), bolt_Parallelism_hint).shuffleGrouping("payment");

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
