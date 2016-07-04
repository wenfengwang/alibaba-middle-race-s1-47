package com.alibaba.middleware.race.test;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wangwenfeng on 7/4/16.
 */
public class AnalyseResult {
    private static final ConcurrentHashMap<Long,Double> producerTaobaoOrder = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Long,Double> producerTmallOrder = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Long,double[]> producerPayment = new ConcurrentHashMap<>();

    private final TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairServerAddr, RaceConfig.TairNamespace);
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    private BufferedWriter bw;
    private volatile static long startTime = 0;

    public AnalyseResult(String path) {
        try {
            bw = new BufferedWriter(new FileWriter(new File(path)));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void addOrder(OrderMessage orderMessage, short platform) { // 0 taobao 1 tmall
        try {
            long timeStamp = sdf.parse(sdf.format(new Date(orderMessage.getCreateTime()))).getTime()/1000;
            if (platform == 0) {
                double totalPrice = producerTaobaoOrder.get(timeStamp) + orderMessage.getTotalPrice();
                producerTaobaoOrder.put(timeStamp,totalPrice);
            } else {
                double totalPrice = producerTmallOrder.get(timeStamp) + orderMessage.getTotalPrice();
                producerTmallOrder.put(timeStamp,totalPrice);
            }


        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

    public void addPayment(PaymentMessage paymentMessage) {
        long timeStamp = 0;
        try {
            timeStamp = sdf.parse(sdf.format(new Date(paymentMessage.getCreateTime()))).getTime()/1000;
            double[] amountArr = producerPayment.get(timeStamp);
            if (amountArr == null) {
                amountArr = new double[]{0,0};
            }

            short platForm = paymentMessage.getPayPlatform();
            amountArr[platForm] += paymentMessage.getPayAmount();
            producerPayment.put(timeStamp,amountArr);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }


    private void analyseTaobao() throws IOException, InterruptedException {
        Thread.sleep(5000);
        Set<Map.Entry<Long, Double>> tbEntrySet = producerTaobaoOrder.entrySet();
        HashMap<Long, String> tbResultMap = new HashMap<>();
        int tbEntrySetSize = tbEntrySet.size();
        float tbSuccess = 0;
        for (Map.Entry entry : tbEntrySet) {
            long timeStamp = (long) entry.getKey();
            double tairTaobaoPrice = (double) tairOperator.get(RaceConfig.prex_taobao+timeStamp);

            String str;
            if (tairTaobaoPrice == (double) entry.getValue()) {
                tbSuccess++;
                str = RaceConfig.prex_taobao+timeStamp + ", Result: Success. Amount is " + String.valueOf(tairTaobaoPrice);
            } else {
                str = RaceConfig.prex_taobao+timeStamp + ", Result: Failed. Tair: " + tairTaobaoPrice +
                                                                ", Producer: "+ entry.getValue();
            }
            tbResultMap.put(timeStamp,str);
            bw.write(str);
        }
        bw.write("Taobao准确率: " + tbSuccess/tbEntrySetSize);
        bw.flush();
        bw.close();
        System.out.println("Taobao准确率: " + tbSuccess/tbEntrySetSize);
    }

    private void analyseTmall() throws IOException, InterruptedException {
        Thread.sleep(5000);
        Set<Map.Entry<Long, Double>> tmEntrySet = producerTmallOrder.entrySet();
        HashMap<Long, String> tmResultMap = new HashMap<>();
        int tmEntrySetSize = tmEntrySet.size();
        float tmSuccess = 0;
        for (Map.Entry entry : tmEntrySet) {
            long timeStamp = (long) entry.getKey();
            double tairTaobaoPrice = (double) tairOperator.get(RaceConfig.prex_tmall+timeStamp);

            String str;
            if (tairTaobaoPrice == (double) entry.getValue()) {
                tmSuccess++;
                str = RaceConfig.prex_tmall+timeStamp + ", Result: Success. Amount is " + String.valueOf(tairTaobaoPrice);
            } else {
                str = RaceConfig.prex_tmall+timeStamp + ", Result: Failed. Tair: " + tairTaobaoPrice +
                        ", Producer: "+ entry.getValue();
            }
            tmResultMap.put(timeStamp,str);
            bw.write(str);
        }
        bw.write("Tmall准确率: " + tmSuccess/tmEntrySetSize);
        bw.flush();
        bw.close();
        System.out.println("Tmall准确率: " + tmSuccess/tmEntrySetSize);
    }

    private void analysePayment() throws IOException, InterruptedException {
        Thread.sleep(5000);
        double pcTotalPrice = 0;
        double moTotalPrice = 0;
        Set<Map.Entry<Long, double[]>> EntrySet = producerPayment.entrySet();
        HashMap<Long, String> ResultMap = new HashMap<>();
        int EntrySetSize = EntrySet.size();
        float success = 0;
        for (Map.Entry<Long, double[]> entry : EntrySet) {
            long timeStamp = entry.getKey();
            double tairRatio = (double) tairOperator.get(RaceConfig.prex_ratio+timeStamp);
            pcTotalPrice += entry.getValue()[0];
            moTotalPrice += entry.getValue()[1];
            double ratio = moTotalPrice/pcTotalPrice;
            String str;
            if (tairRatio == ratio) {
                success++;
                str = RaceConfig.prex_ratio+timeStamp + ", Result: Success. Ratio is " + tairRatio;
            } else {
                str = RaceConfig.prex_ratio+timeStamp + ", Result: Failed. Tair: " + tairRatio +
                        ", Producer: "+ ratio;
            }
            ResultMap.put(timeStamp,str);
            bw.write(str);
        }
        bw.write("支付信息准确率: " + success/EntrySetSize);
        bw.flush();
        bw.close();
        System.out.println("支付信息准确率: " + success/EntrySetSize);
    }

    public void startTime(){
        if (startTime == 0)
            startTime = System.currentTimeMillis();
    }

}
