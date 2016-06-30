package com.alibaba.middleware.race.jstorm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.client.spout.IAckValueSpout;
import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
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
        LOG.warn("不支持的方法调用");
    }

    @Override
    public void deactivate() {
        LOG.warn("不支持的方法调用");
    }

    @Override
    public void nextTuple() {

        try {
            Set<MessageQueue> messageQueueSet = consumer.fetchSubscribeMessageQueues(topic);
            if (messageQueueSet == null) {
                return;
            }
            for (MessageQueue mq : messageQueueSet) {
                //每个队列里无限循环，分批拉取未消费的消息，直到拉取不到新消息为止
                long offset = consumer.fetchConsumeOffset(mq, false);
                offset = offset < 0 ? 0 : offset;
                PullResult result = consumer.pull(mq, null, offset, 64);

                switch (result.getPullStatus()) {
                    case FOUND:
//                        System.out.println("消费进度 topic: " + topic + "; Offset: " + offset);
//                        System.out.println("接收到的消息集合, topic: " + topic + "; " + result);
                        List<MessageExt> list = result.getMsgFoundList();
                        MqTuple mqTuple = new MqTuple(list);
                        if (list == null || list.size() == 0) {
                            break;
                        }

                        sendTuple(mqTuple);
                        // 获取下一个下标位置
                        // TODO 为啥
                        offset = result.getNextBeginOffset();
                        consumer.updateConsumeOffset(mq, offset);
                        break;
                    case NO_MATCHED_MSG:
                        System.out.println("没有匹配的消息");
                        break;
                    case NO_NEW_MSG:
//                        System.out.println("没有未消费的新消息");
                        //拉取不到新消息，跳出 SINGLE_MQ 当前队列循环，开始下一队列循环。
                        break;
                    case OFFSET_ILLEGAL:
                        System.out.println("下标错误");
                        break;
                    default:
                        break;
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println("***** "+ topic + "*****");
            e.printStackTrace();
        }
    }

    @Override
    public void ack(Object msgId) {
        LOG.warn("不支持的方法调用");
    }

    @Override
    public void fail(Object msgId) {
        LOG.warn("不支持的方法调用");
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
            LOG.warn("Message " + mqTuple.getMq() + " fail times " + failNum);
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
