package com.alibaba.middleware.race.model;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;

/**
 * Created by wangwenfeng on 7/1/16.
 */
public class Ratio {

    private final long timeStamp; // 整分时间戳
    private final String  prex;
    private volatile double ratio; // 比值
    public volatile boolean toBeTair = false;

    private volatile double currentPCAmount; // 当前整分时刻内PC端的量
    private volatile double currentMobileAmount; // 当前整分时刻内移动端的量
    private volatile double PCAmount;    // 当前时刻的PC端总金额
    private volatile double MobileAmount;    // 当前时刻的手机端总金额

    private Ratio preRatio;
    private Ratio nextRtaio;

    public Ratio(long timeStamp, Ratio preRatio) {
        if (preRatio.timeStamp >= timeStamp) {
            throw new RuntimeException("timeStamp error");
        }
        this.timeStamp = timeStamp;
        this.currentPCAmount = 0;
        this.currentMobileAmount = 0;
        this.preRatio = preRatio;
        prex = RaceConfig.prex_ratio + timeStamp;

        if (preRatio == null) {
            PCAmount = 0;
            MobileAmount = 0;
            ratio = 0;
            this.nextRtaio = null;
            return;
        }
        PCAmount = preRatio.PCAmount;
        MobileAmount = preRatio.MobileAmount;
        ratio = preRatio.ratio;

        if (preRatio.nextRtaio != null) {
            if (preRatio.nextRtaio.getTimeStamp() <= timeStamp) {
                throw new RuntimeException("preRatio's nextRatio error");
            }
            preRatio.getNextRtaio().setPreRatio(this);
            this.nextRtaio = preRatio.getNextRtaio();
            preRatio.setNextRtaio(this);
        } else {
            preRatio.setNextRtaio(this);
            this.nextRtaio = null;
        }
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public double getCurrentPCAmount() {
        return currentPCAmount;
    }

    public double getCurrentMobileAmount() {
        return currentMobileAmount;
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

    public void setNextRtaio(Ratio nextRtaio) {
        this.nextRtaio = nextRtaio;
    }

    public void updateMobileAmount(double v) {
        currentMobileAmount += v;
        MobileAmount += v;
        updatedRatio();
    }

    public void updatePCAmount(double v) {
        currentPCAmount += v;
        PCAmount += v;
        updatedRatio();
    }

    private void updatedRatio() {
        if (MobileAmount == 0 || PCAmount == 0 ){
            return;
        }
        ratio = MobileAmount/PCAmount;

        if (!toBeTair)
            toBeTair = true;
    }

    public void toTair(TairOperatorImpl tairOperator) {
        tairOperator.write(prex,ratio);
        toBeTair = false;
    }
}
