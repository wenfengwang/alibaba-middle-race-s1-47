package com.alibaba.middleware.race.jstorm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by wangwenfeng on 5/27/16.
 *
 */
public class TaobaoTopicSpout implements IRichSpout {
    private static Logger LOG = LoggerFactory.getLogger(TaobaoTopicSpout.class);
    private SpoutOutputCollector _collector;
    private static DefaultMQPullConsumer pullConsumer;
//    private long startTime;
    private static final Map<MessageQueue, Long> offseTable = new HashMap<MessageQueue, Long>();

    public TaobaoTopicSpout() throws MQClientException {
        pullConsumer =  new DefaultMQPullConsumer(RaceConfig.MetaConsumerGroup);
        pullConsumer.setNamesrvAddr(RaceConfig.MQNameServerAddr);
        pullConsumer.start();
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        LOG.info("TaobaoTopicSpout started");
    }

    @Override
    public void nextTuple() {
        try {
            LOG.info("************ begin tuple ************");
            Set<MessageQueue> msq = pullConsumer.fetchSubscribeMessageQueues(RaceConfig.MqTaobaoTradeTopic);
            Iterator<MessageQueue> iter = msq.iterator();
            MessageQueue mq;
            while (iter.hasNext()) {
                mq = iter.next();
                // TODO 这个offset到底特么是干嘛的
                PullResult pullResult = pullConsumer.pullBlockIfNotFound(mq,
                        null,getMessageQueueOffset(mq),32);
                putMessageQueueOffset(mq,pullResult.getNextBeginOffset());
                switch (pullResult.getPullStatus()) {
                    case FOUND:
                        List list = pullResult.getMsgFoundList();
                        _collector.emit(list);
                        pullConsumer.updateConsumeOffset(mq,getMessageQueueOffset(mq));
                        break;
                    case NO_MATCHED_MSG:
                        break;
                    case NO_NEW_MSG:
                        break;
                    case OFFSET_ILLEGAL:
                        break;
                    default:
                        break;
                }
            }
//            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        } finally {
            LOG.info("************ end tuple ************");
        }
    }


    private static void putMessageQueueOffset(MessageQueue mq, long offset) {
        offseTable.put(mq, offset);
    }

    private static long getMessageQueueOffset(MessageQueue mq) {
        Long offset = offseTable.get(mq);
        if (offset != null)
            return offset;
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void ack(Object msgId) {

    }

    @Override
    public void fail(Object msgId) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
