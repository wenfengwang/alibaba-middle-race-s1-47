package com.alibaba.middleware.race.jstorm.spout;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.MQHelper;
import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by wangwenfeng on 5/31/16.
 */
public class ConsumerFactory {
    private static Logger LOG = LoggerFactory.getLogger(RaceSpout.class);


    public static Map<String, DefaultMQPushConsumer> consumers =
            new HashMap<String, DefaultMQPushConsumer>();

    public static DefaultMQPullConsumer pullConsumer;

    public static synchronized DefaultMQPushConsumer mkInstance(SpoutConfig config,
                                                                MessageListenerConcurrently listener)  throws Exception{

        String groupId = config.getConsumerGroup();

        String key = groupId;

        DefaultMQPushConsumer consumer = consumers.get(key);
        if (consumer != null) {
            LOG.info("Consumer of " + key + " has been created, don't recreate it ");
            return consumer;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Begin to init meta client \n");
        sb.append(",configuration:").append(config);

        LOG.info(sb.toString());

        consumer = new DefaultMQPushConsumer(config.getConsumerGroup());

        consumer.setNamesrvAddr(config.getNameServer());

        String instanceName = groupId +"@" +	JStormUtils.process_pid();
        consumer.setInstanceName(instanceName);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe(RaceConfig.MqPayTopic, config.getSubExpress());
        consumer.subscribe(RaceConfig.MqTmallTradeTopic, config.getSubExpress());
        consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, config.getSubExpress());

        consumer.registerMessageListener(listener);

        consumer.setPullThresholdForQueue(config.getQueueSize());
        consumer.setConsumeMessageBatchMaxSize(config.getSendBatchSize());
        consumer.setPullBatchSize(config.getPullBatchSize());
        consumer.setPullInterval(config.getPullInterval());
        consumer.setConsumeThreadMin(config.getPullThreadNum());
        consumer.setConsumeThreadMax(config.getPullThreadNum());


        Date date = config.getStartTimeStamp() ;
        if ( date != null) {
            LOG.info("Begin to reset meta offset to " + date);
            try {
                MQHelper.resetOffsetByTimestamp(MessageModel.CLUSTERING,
                        instanceName, config.getConsumerGroup(),
                        config.getTopic(), date.getTime());
                LOG.info("Successfully reset meta offset to " + date);
            }catch(Exception e) {
                LOG.error("Failed to reset meta offset to " + date);
            }

        }else {
            LOG.info("Don't reset meta offset  ");
        }

        consumer.start();

        consumers.put(key, consumer);
        LOG.info("Successfully create " + key + " consumer");

        return consumer;
    }

    public static synchronized DefaultMQPullConsumer mkPullInstance(String topic) throws MQClientException {
        if (pullConsumer == null) {
            pullConsumer = new DefaultMQPullConsumer(RaceConfig.MqConsumerGroup);
            if (!RaceConfig.ONLINE)
                pullConsumer.setNamesrvAddr(RaceConfig.MQNameServerAddr);

            pullConsumer.start();
            LOG.info("Successfully create pullConsumer");
        }

        return pullConsumer;
    }

}
