package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.blot.CountTaobao;
import com.alibaba.middleware.race.jstorm.spout.TaobaoTopicSpout;
import com.alibaba.rocketmq.client.exception.MQClientException;
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

        HashMap conf = new HashMap();
        conf.put(Config.TOPOLOGY_WORKERS, 2);

        int spout_Parallelism_hint = 1;
        int bolt_Parallelism_hint = 2;

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("taobao", new TaobaoTopicSpout(), spout_Parallelism_hint);
//        builder.setSpout("tmall", new TmallTopicSpout(), spout_Parallelism_hint);
//        builder.setSpout("pay", new PaymenyTopicSpout(), spout_Parallelism_hint);

        builder.setBolt("countTaobao", new CountTaobao(), bolt_Parallelism_hint).shuffleGrouping("taobao");
//        builder.setBolt("presistTaobao", new PersistTaobao(), bolt_Parallelism_hint).shuffleGrouping("countTaobao");
        String topologyName = RaceConfig.JstormTopologyName;
        try {
            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
            LOG.info("Topology submittedsssssssssssssssssss");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
