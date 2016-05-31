package com.alibaba.middleware.race.jstorm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.client.spout.IAckValueSpout;
import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by wangwenfeng on 5/31/16.
 */
public class RaceSpout implements IRichSpout, MessageListenerConcurrently, IAckValueSpout, IFailValueSpout {
    private static Logger LOG = LoggerFactory.getLogger(RaceSpout.class);
    protected SpoutConfig mqClientConfig;

    private LinkedBlockingDeque<MqTuple> sendingQueue;
    private SpoutOutputCollector collector;
    private transient DefaultMQPushConsumer consumer;

    private Map conf;
    protected String id;
    private String field;
    private boolean flowControl = true;
    private boolean autoAck = false;

    public RaceSpout(String field) {
        this.field = field;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.conf = conf;
        this.collector = collector;
        this.id = context.getThisComponentId() + ":" + context.getThisTaskId();
        this.sendingQueue = new LinkedBlockingDeque<MqTuple>();
    }

    @Override
    public void close() {
        if (consumer != null)
            consumer.shutdown();
    }

    @Override
    public void activate() {
        if (consumer != null)
            consumer.resume();
    }

    @Override
    public void deactivate() {
        if (consumer != null)
            consumer.suspend();
    }

    @Override
    public void nextTuple() {
        MqTuple mqTuple = null;
        try {
            mqTuple = sendingQueue.take();
        } catch (InterruptedException e) {
        }

        if (mqTuple == null) {
            return;
        }

        sendTuple(mqTuple);
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
        declarer.declare(new Fields(this.field));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        try {
            MqTuple mqTuple = new MqTuple(msgs, context.getMessageQueue());

            if (flowControl) {
                sendingQueue.offer(mqTuple);
            } else {
                sendTuple(mqTuple);
            }

            if (autoAck) {
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            } else {
                mqTuple.waitFinish();
                if (mqTuple.isSuccess() == true) {
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                } else {
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }

        } catch (Exception e) {
            LOG.error("Failed to emit " + id, e);
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
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


        if (failNum > mqClientConfig.getMaxFailTimes()) {
            LOG.warn("Message " + mqTuple.getMq() + " fail times " + failNum);
            finishTuple(mqTuple);
            return;
        }

        if (flowControl) {
            sendingQueue.offer(mqTuple);
        } else {
            sendTuple(mqTuple);
        }
    }
    private void sendTuple(MqTuple mqTuple) {
        mqTuple.updateEmitMs();
        collector.emit(new Values(mqTuple), mqTuple.getCreateMs());
    }

    public void finishTuple(MqTuple mqTuple) {
//        waithHistogram.update(metaTuple.getEmitMs() - metaTuple.getCreateMs());
//        processHistogram.update(System.currentTimeMillis() - metaTuple.getEmitMs());
        mqTuple.done();
    }
}
