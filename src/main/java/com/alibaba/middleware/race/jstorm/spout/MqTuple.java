package com.alibaba.middleware.race.jstorm.spout;

import backtype.storm.task.ICollectorCallback;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by wangwenfeng on 5/31/16.
 */
public class MqTuple implements Serializable {

    protected final List<MessageExt> msgList;
    protected final MessageQueue mq;

    protected final AtomicInteger failureTimes;
    protected final long createMs;
    protected long emitMs;

    protected transient CountDownLatch latch;
    protected transient boolean isSuccess;

    public MqTuple() {
        msgList = null;
        mq = null;
        createMs = System.currentTimeMillis();
        failureTimes = new AtomicInteger(0);
        latch = new CountDownLatch(1);
        isSuccess = false;
    }

    public MqTuple(List<MessageExt> msgs, MessageQueue messageQueue) {
        msgList = msgs;
        mq = messageQueue;
        createMs = System.currentTimeMillis();
        failureTimes = new AtomicInteger(0);
        latch = new CountDownLatch(1);
        isSuccess = false;
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    public AtomicInteger getFailureTimes() {
        return failureTimes;
    }

    public MessageQueue getMq() {
        return mq;
    }

    public void done() {
        this.isSuccess = true;
        latch.countDown();
    }

    public void fail() {
        isSuccess = false;
        latch.countDown();
    }

    public long getCreateMs() {
        return createMs;
    }

    public void updateEmitMs() {
        emitMs = System.currentTimeMillis();
    }

    public long getEmitMs() {
        return emitMs;
    }

    public List<MessageExt> getMsgList() {
        return msgList;
    }

    public boolean waitFinish() throws InterruptedException {
        return latch.await(4, TimeUnit.HOURS);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this,
                ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
