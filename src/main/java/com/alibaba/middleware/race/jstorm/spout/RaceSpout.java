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
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by wangwenfeng on 5/31/16.
 */
public class RaceSpout implements IRichSpout, MessageListenerConcurrently, IAckValueSpout, IFailValueSpout {
    private static Logger LOG = LoggerFactory.getLogger(RaceSpout.class);
    private SpoutConfig mqClientConfig;
    private LinkedBlockingDeque<MqTuple> sendingQueue;
    private SpoutOutputCollector collector;
    private static DefaultMQPushConsumer consumer;
    private AtomicInteger count = new AtomicInteger(0);

    private Map tpConf;
    private Map spoutConf;
    protected String id;
    private static final boolean flowControl = false;
    private static final boolean autoAck = true;
    public RaceSpout(){}
    public RaceSpout(Map conf) {
        this.spoutConf = conf;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.tpConf = conf;
        this.collector = collector;
        this.id = context.getThisComponentId() + ":" + context.getThisTaskId();
        this.sendingQueue = new LinkedBlockingDeque<>();

        tpConf.putAll(spoutConf);
        mqClientConfig = SpoutConfig.mkInstance(conf);

        try {
            consumer = ConsumerFactory.mkInstance(mqClientConfig, this);
        } catch (Exception e) {
            LOG.error("Failed to create Meta Consumer ", e);
            throw new RuntimeException("Failed to create MetaConsumer" + id, e);
        }

        if (consumer == null) {
            LOG.warn(id + " already exist consumer in current worker, don't need to fetch data ");
        }
        count.addAndGet(1);
        LOG.info("Successfully init " + id);
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
            e.printStackTrace();
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
        declarer.declareStream(RaceConfig.TAOBAO_STREAM_ID,new Fields("taobao"));
        declarer.declareStream(RaceConfig.TMALL_STREAM_ID,new Fields("tmall"));
        declarer.declareStream(RaceConfig.PAYMENT_STREAM_ID,new Fields("payment"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        try {
            MqTuple mqTuple = new MqTuple(new ArrayList<>(msgs), context.getMessageQueue());
            if (flowControl) {
                sendingQueue.offer(mqTuple);
            } else {
                sendTuple(mqTuple);
            }

            if (autoAck) {
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            } else {
                mqTuple.waitFinish();
                if (mqTuple.isSuccess()) {
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
        switch (mqTuple.getMq().getTopic()) {
            case RaceConfig.TAOBAO_STREAM_ID:
                collector.emit(RaceConfig.TAOBAO_STREAM_ID,new Values(mqTuple), mqTuple.getCreateMs());
                break;
            case RaceConfig.TMALL_STREAM_ID:
                collector.emit(RaceConfig.TMALL_STREAM_ID,new Values(mqTuple), mqTuple.getCreateMs());
                break;
            case RaceConfig.PAYMENT_STREAM_ID:
                collector.emit(RaceConfig.PAYMENT_STREAM_ID,new Values(mqTuple), mqTuple.getCreateMs());
                break;
        }
    }

    private void finishTuple(MqTuple mqTuple) {
        mqTuple.done();
    }
}
