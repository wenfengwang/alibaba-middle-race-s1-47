package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
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
            if (RaceConfig.TOPOLOGY_MODEL.equals("cluster")) {
                StormSubmitter.submitTopology(RaceConfig.JstormTopologyName, tpConf, setBuilderWithPush());
            } else {
                LocalCluster localCluster = new LocalCluster();
                localCluster.submitTopology(RaceConfig.JstormTopologyName, tpConf, setBuilderWithPush());
                Thread.sleep(100000000);
                localCluster.shutdown();
                LOG.info("Topology submitted!!!!");
            }
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
        HashMap<Object, Object> conf = new HashMap();
        conf.put(SpoutConfig.META_CONSUMER_GROUP, RaceConfig.MqConsumerGroup);
        conf.put(SpoutConfig.META_BATCH_PULL_MSG_SIZE,64);
        conf.put(SpoutConfig.META_BATCH_SEND_MSG_SIZE,64);
        if (!RaceConfig.ONLINE)
            conf.put(SpoutConfig.META_NAMESERVER,RaceConfig.MQNameServerAddr);

        int spout_Parallelism_hint = 3;
        int high_Parallelism_hint  = 3;
        int mid_Parallelism_hint   = 2;
        int low_Parallelism_hint   = 1;

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(RaceConfig.SPOUT_NAME,new RaceSpout(conf), spout_Parallelism_hint);

        // 消息序列化bolt todo 提高了bolt的并发数,观察下性能效果
        builder.setBolt(RaceConfig.TAOBAO_COUNT_BOLT_ID, new CountBolt(), high_Parallelism_hint)
                .shuffleGrouping(RaceConfig.SPOUT_NAME,RaceConfig.TAOBAO_STREAM_ID);
        builder.setBolt(RaceConfig.TMALL_COUNT_BOLT_ID, new CountBolt(), high_Parallelism_hint)
                .shuffleGrouping(RaceConfig.SPOUT_NAME,RaceConfig.TMALL_STREAM_ID);
        builder.setBolt(RaceConfig.RATIO_SPLIT_BOLT_ID, new RatioBolt(), high_Parallelism_hint)
                .shuffleGrouping(RaceConfig.SPOUT_NAME,RaceConfig.PAYMENT_STREAM_ID);

        // 处理订单号和支付信息
        builder.setBolt(RaceConfig.ORDER_TRANS_BOLT, new OrderProcessBolt(),low_Parallelism_hint)
                .shuffleGrouping(RaceConfig.TAOBAO_COUNT_BOLT_ID)
                .shuffleGrouping(RaceConfig.TMALL_COUNT_BOLT_ID)
                .shuffleGrouping(RaceConfig.RATIO_SPLIT_BOLT_ID, RaceConfig.PAY_ORDER_STREAM_ID);

        // 写订单金额
        builder.setBolt(RaceConfig.TAOBAO_PERSIST_BOLT_ID, new PersistBolt(RaceConfig.prex_taobao),low_Parallelism_hint)
                .shuffleGrouping(RaceConfig.ORDER_TRANS_BOLT,RaceConfig.TAOBAO_PERSIST_STREAM_ID);
        builder.setBolt(RaceConfig.TMALL_PERSIST_BOLT_ID, new PersistBolt(RaceConfig.prex_tmall),low_Parallelism_hint)
                .shuffleGrouping(RaceConfig.ORDER_TRANS_BOLT,RaceConfig.TMALL_PERSIST_STREAM_ID);

        // PC MOBILE 比例计算
        builder.setBolt(RaceConfig.RATIO_COUNT_BOLT_ID, new RatioCount(),mid_Parallelism_hint)
                .shuffleGrouping(RaceConfig.RATIO_SPLIT_BOLT_ID, RaceConfig.PAY_STREAM_ID);
        builder.setBolt(RaceConfig.RATIO_PERSIST_BOLT_ID, new PersistRatio(),low_Parallelism_hint)
                .shuffleGrouping(RaceConfig.RATIO_COUNT_BOLT_ID);

        return builder.createTopology();
    }
}
