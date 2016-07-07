package com.alibaba.middleware.race.jstorm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.client.spout.IAckValueSpout;
import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.alibaba.middleware.race.test.AnalyseResult;
import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by sxian.wang on 2016/6/30.
 */
public class RaceSpoutPull implements IRichSpout, IAckValueSpout, IFailValueSpout {
    private static Logger LOG = LoggerFactory.getLogger(RaceSpout.class);
    private SpoutOutputCollector collector;

    private static DefaultMQPullConsumer consumer;

    private String topic;
    protected String id;

    public RaceSpoutPull(String topic) {
        this.topic = topic;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)  {
        try {
            /**
             * TODO
             * 用这个写法会一直进到if里面
             * if(consumer == null) {
             *      consumer = new DefaultMQPullConsumer(RaceConfig.MqConsumerGroup);
             *      consumer.setNamesrvAddr(RaceConfig.MQNameServerAddr);
             *      consumer.start();
             * }
             */
            consumer = ConsumerFactory.mkPullInstance(topic);
            consumer.registerMessageQueueListener(topic, null);
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        this.collector = collector;
        this.id = context.getThisComponentId() + ":" + context.getThisTaskId();

        LOG.info("Successfully init " + id);
    }

    @Override
    public void close() {
        if (consumer != null)
            consumer.shutdown();
    }

    @Override
    public void activate() {
        LOG.warn("activate: unsupport method!");
    }

    @Override
    public void deactivate() {
        LOG.warn("deactivate: unsupport method!");
    }

    @Override
    public void nextTuple() {
        try {
            Set<MessageQueue> messageQueueSet = consumer.fetchSubscribeMessageQueues(topic);

            for (MessageQueue mq : messageQueueSet) {
                long offset = consumer.fetchConsumeOffset(mq, false);
                offset = offset < 0 ? 0 : offset;
                PullResult result = consumer.pull(mq, null, offset, 64);

                switch (result.getPullStatus()) {
                    case FOUND:
                        ArrayList<byte[]> emitlist = new ArrayList<>();
                        List<MessageExt> list = result.getMsgFoundList();
                        if (list == null || list.size() == 0) {
                            break;
                        }
                        for (int i = 0; i<list.size();i++) {
                            emitlist.add(list.get(i).getBody());
                        }
                        MqTuple mqTuple = new MqTuple(emitlist,topic);
                        sendTuple(mqTuple);
                        // 获取下一个下标位置
                        // TODO 为啥
                        offset = result.getNextBeginOffset();
                        consumer.updateConsumeOffset(mq, offset);
                        break;
                    case NO_MATCHED_MSG:
                        LOG.info("NO_MATCHED_MSG");
                        Thread.sleep(200);
                        break;
                    case NO_NEW_MSG:
                        LOG.info("***** "+ topic + "*****"+"NO_NEW_MSG");
                        Thread.sleep(200);
                        break;
                    case OFFSET_ILLEGAL:
                        LOG.info("OFFSET_ILLEGAL");
                        Thread.sleep(200);
                        break;
                    default:
                        LOG.info("NOTHING");
                        Thread.sleep(200);
                        break;
                }
            }
        } catch (Exception e) {
            LOG.info("***** "+ topic + "*****");
            LOG.error(e.getMessage());
        }
    }

    @Override
    public void ack(Object msgId) {
        LOG.warn("ack: unsupport method!");
    }

    @Override
    public void fail(Object msgId) {
        LOG.warn("fail: unsupport method!");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(topic));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
    @Override
    public void ack(Object msgId, List<Object> values) {
        MqTuple metaTuple = (MqTuple) values.get(0);
        finishTuple(metaTuple);
    }

    @Override
    public void fail(Object msgId, List<Object> values) {
        MqTuple mqTuple = (MqTuple) values.get(0);
        AtomicInteger failTimes = mqTuple.getFailureTimes();

        int failNum = failTimes.incrementAndGet();

        if (failNum > 5) {
            LOG.warn("Message " + mqTuple.getTopic()+ ": " + mqTuple.getCreateMs() + " fail times " + failNum);

            finishTuple(mqTuple);
            return;
        }

        sendTuple(mqTuple);
    }
    private void sendTuple(MqTuple mqTuple) {
        mqTuple.updateEmitMs();
        collector.emit(new Values(mqTuple), mqTuple.getCreateMs());
    }

    public void finishTuple(MqTuple mqTuple) {
        mqTuple.done();
    }
}
