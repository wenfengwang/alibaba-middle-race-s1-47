package com.alibaba.middleware.race.model;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;

import java.io.Serializable;
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
    private volatile long lastUpdate = -1;
    private volatile long lastToTair = -1;

    private volatile double currentPCAmount; // 当前整分时刻内PC端的量
    private volatile double currentMobileAmount; // 当前整分时刻内移动端的量
    private volatile double PCAmount;    // 当前时刻的PC端总金额
    private volatile double MobileAmount;    // 当前时刻的手机端总金额

    private Ratio preRatio;
    private Ratio nextRtaio;

    public Ratio(long timeStamp, Ratio preRatio) {
        createTime = System.currentTimeMillis();
        this.timeStamp = timeStamp;
        this.currentPCAmount = 0;
        this.currentMobileAmount = 0;
        this.preRatio = preRatio;
        key = RaceConfig.prex_ratio + timeStamp;

        if (preRatio == null) {
            PCAmount = 0;
            MobileAmount = 0;
            ratio = 0;
            this.nextRtaio = null;
            return;
        }
        PCAmount = preRatio.getPCAmount();
        MobileAmount = preRatio.getMobileAmount();
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
        this.currentPCAmount = 0;
        this.currentMobileAmount = 0;
        key = RaceConfig.prex_ratio + timeStamp;
        switch (flag) {
            case 0:
                this.ratio = 0; // TODO ratio在构造器里面设置不设置无所谓?
                preRatio = null;
                nextRtaio = ratio;
                ratio.setPreRatio(this);
                PCAmount = 0;
                MobileAmount = 0;
                break;
            case 1:
                this.ratio = ratio.getRatio();
                preRatio = ratio;
                nextRtaio = null;
                ratio.setNextRtaio(this);
                PCAmount = ratio.getPCAmount();
                MobileAmount = ratio.getMobileAmount();
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
        return PCAmount;
    }

    public double getMobileAmount() {
        return MobileAmount;
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

    public String getKey() {
        return key;
    }

    public double getResult() {
        return ratio;
    }

    public void setNextRtaio(Ratio nextRtaio) {
        this.nextRtaio = nextRtaio;
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getLastToTair() {
        return lastToTair;
    }
    public void updateCurrentAmount(double[] amount) {  //double pc, double mobile
        synchronized (this) {
            currentPCAmount += amount[0];
            currentMobileAmount += amount[1];
        }
        updateAmount(amount);

        Ratio ratio = nextRtaio;
        while (ratio != null) {
            ratio.updateAmount(amount);
            ratio = ratio.getNextRtaio();
        }
    }

    private synchronized void updateAmount(double[] amount) {
        synchronized (this) {
            PCAmount += amount[0];
            MobileAmount += amount[1];
            lastUpdate = System.currentTimeMillis();
            if (!toBeTair.get())
                toBeTair.set(true);
        }
        System.out.println("----------------------------------------------");
    }

    public void toTair(TairOperatorImpl tairOperator) {
        if (toBeTair.get() && lastUpdate > lastToTair) {
            writeRatio(tairOperator);
        }

        Ratio ratio = nextRtaio;

        while (ratio != null) {
            ratio.writeRatio(tairOperator);
            ratio = ratio.getNextRtaio();
        }
    }

    public void writeRatio(TairOperatorImpl tairOperator) {
        synchronized (this) {
            ratio = (MobileAmount == 0 || PCAmount == 0) ? 0 : MobileAmount/PCAmount;
            tairOperator.write(key,ratio);
            lastToTair = System.currentTimeMillis();
            toBeTair.set(false);
        }
        System.out.println("**********************************************");
    }

}
