package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import clojure.lang.Obj;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.blot.CountTaobao;
import com.alibaba.middleware.race.jstorm.spout.RaceSpout;
import com.alibaba.middleware.race.jstorm.spout.SpoutConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;


/**
 * 这是一个很简单的例子
 * 选手的拓扑提交到集群，我们是有超时设置的。每个选手的拓扑最多跑20分钟，一旦超过这个时间
 * 我们会将选手拓扑杀掉。
 */

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

        int spout_Parallelism_hint = 1;
        int bolt_Parallelism_hint = 2;

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("taobao",new RaceSpout(confTaobao), spout_Parallelism_hint);
//        builder.setSpout("tmall",new RaceSpout(confTmall), spout_Parallelism_hint);
//        builder.setSpout("payment",new RaceSpout(confPayment), spout_Parallelism_hint);


        builder.setBolt("countTaobao", new CountTaobao(), bolt_Parallelism_hint).shuffleGrouping("taobao");
//        builder.setBolt("countTmall", new CountTaobao(), bolt_Parallelism_hint).shuffleGrouping("tmall");
//        builder.setBolt("countPayment", new CountTaobao(), bolt_Parallelism_hint).shuffleGrouping("payment");

        try {
//            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
           LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology(RaceConfig.JstormTopologyName, tpConf, builder.createTopology());
            Thread.sleep(100000);
            localCluster.shutdown();
//            LOG.info("Topology submitted!!!!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
