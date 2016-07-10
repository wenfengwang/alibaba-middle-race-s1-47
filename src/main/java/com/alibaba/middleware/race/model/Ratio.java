package com.alibaba.middleware.race.model;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.google.common.util.concurrent.AtomicDouble;

import java.io.Serializable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by wangwenfeng on 7/1/16.
 */
public class Ratio implements Serializable{
    private final long timeStamp; // 整分时间戳
    private final String  key;
    private volatile double ratio; // 比值
    public AtomicBoolean toBeTair = new AtomicBoolean(false);
    public final long createTime;

    private volatile AtomicDouble currentPCAmount; // 当前整分时刻内PC端的量
    private volatile AtomicDouble currentMobileAmount; // 当前整分时刻内移动端的量
    private volatile AtomicDouble PCAmount;    // 当前时刻的PC端总金额
    private volatile AtomicDouble MobileAmount;    // 当前时刻的手机端总金额

    private Ratio preRatio;
    private Ratio nextRtaio;

    public Ratio(long timeStamp, Ratio preRatio) {
        createTime = System.currentTimeMillis();
        this.timeStamp = timeStamp;
        this.currentPCAmount = new AtomicDouble(0);
        this.currentMobileAmount = new AtomicDouble(0);
        this.preRatio = preRatio;
        key = RaceConfig.prex_ratio + timeStamp;

        if (preRatio == null) {
            PCAmount = new AtomicDouble(0);
            MobileAmount = new AtomicDouble(0);
            ratio = 0;
            this.nextRtaio = null;
            return;
        }
        PCAmount = new AtomicDouble(preRatio.getPCAmount());
        MobileAmount = new AtomicDouble(preRatio.getMobileAmount());
        ratio = preRatio.getRatio();

        try {
            // preRatio nextRatio肯定不为null  调用这个构造方法的逻辑保证的
            this.nextRtaio = preRatio.getNextRtaio();
            this.nextRtaio.setPreRatio(this);
            preRatio.setNextRtaio(this);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Ratio(long timeStamp, Ratio ratio, int flag) { //flag位，0是head节点 ，1是tair节点
        createTime = System.currentTimeMillis();
        this.timeStamp = timeStamp;
        this.currentPCAmount = new AtomicDouble(0);
        this.currentMobileAmount = new AtomicDouble(0);
        key = RaceConfig.prex_ratio + timeStamp;
        switch (flag) {
            case 0:
                this.ratio = 0;
                preRatio = null;
                nextRtaio = ratio;
                ratio.setPreRatio(this);
                PCAmount = new AtomicDouble(0);
                MobileAmount = new AtomicDouble(0);
                break;
            case 1:
                this.ratio = ratio.getRatio();
                preRatio = ratio;
                nextRtaio = null;
                ratio.setNextRtaio(this);
                PCAmount = new AtomicDouble(ratio.getPCAmount());
                MobileAmount = new AtomicDouble(ratio.getMobileAmount());
                break;
        }

    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public double getRatio() {
        return ratio;
    }

    public double getPCAmount() {
        return PCAmount.get();
    }

    public double getMobileAmount() {
        return MobileAmount.get();
    }

    public Ratio getPreRatio() {
        return preRatio;
    }

    public void setPreRatio(Ratio preRatio) {
        this.preRatio = preRatio;
    }

    public Ratio getNextRtaio() {
        return nextRtaio;
    }

    public void setNextRtaio(Ratio nextRtaio) {
        this.nextRtaio = nextRtaio;
    }

    public void updateCurrentAmount(double[] amount) {  //double pc, double mobile
        synchronized (this) {
            currentPCAmount.addAndGet(amount[0]);
            currentMobileAmount.addAndGet(amount[1]);
        }
        updateAmount(amount);

        Ratio ratio = nextRtaio;
        while (ratio != null) {
            ratio.updateAmount(amount);
            ratio = ratio.getNextRtaio();
        }
    }

    private void updateAmount(double[] amount) {
            PCAmount.addAndGet(amount[0]);
            MobileAmount.addAndGet(amount[1]);
            if (!toBeTair.get())
                toBeTair.set(true);
    }

    public void toTair(TairOperatorImpl tairOperator,LinkedBlockingQueue ratioQueue) {
        if (toBeTair.get()) {
            writeRatio(tairOperator);
        }

        Ratio ratio = nextRtaio;
        while (ratio != null) {
            ratio.writeRatio(tairOperator);
            ratio = ratio.getNextRtaio();
        }
    }

    public void writeRatio(TairOperatorImpl tairOperator) {
        synchronized (this) { // todo 有没有加同步的必要
            ratio = (MobileAmount.get() == 0 || PCAmount.get() == 0) ? 0 : MobileAmount.get()/PCAmount.get();
            tairOperator.write(key,ratio);
            toBeTair.set(false);
        }
    }

}
