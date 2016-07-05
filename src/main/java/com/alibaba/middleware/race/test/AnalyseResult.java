package com.alibaba.middleware.race.test;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;

import java.io.*;
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

    public AnalyseResult() {
    }

//    public void addOrder(OrderMessage orderMessage, int platform) { // 0 taobao 1 tmall
//        try {
//            long timeStamp = sdf.parse(sdf.format(new Date(orderMessage.getCreateTime()))).getTime()/1000;
//
//            if (platform == 0) {
//                Double totalPrice = producerTaobaoOrder.get(timeStamp);
//                if (totalPrice == null) totalPrice = 0.0;
//                totalPrice += orderMessage.getTotalPrice();
//                producerTaobaoOrder.put(timeStamp,totalPrice);
//            } else {
//                Double totalPrice = producerTmallOrder.get(timeStamp);
//                if (totalPrice == null) totalPrice = 0.0;
//                totalPrice += orderMessage.getTotalPrice();
//                producerTmallOrder.put(timeStamp,totalPrice);
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        System.out.println("added order...");
//    }
//
//    public void addPayment(PaymentMessage paymentMessage) {
//        try {
//            long timeStamp = sdf.parse(sdf.format(new Date(paymentMessage.getCreateTime()))).getTime()/1000;
//            double[] amountArr = producerPayment.get(timeStamp);
//            if (amountArr == null) {
//                amountArr = new double[]{0,0};
//            }
//
//            short platForm = paymentMessage.getPayPlatform();
//            amountArr[platForm] += paymentMessage.getPayAmount();
//            producerPayment.put(timeStamp,amountArr);
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//        System.out.println("added payment...");
//
//    }


    public void analyseTaobao() throws IOException, InterruptedException {
        Thread.sleep(5000);
        BufferedWriter tb_bw_result = new BufferedWriter(new FileWriter(new File("E:\\mdw_data\\tb_result.txt")));
        BufferedReader tb_br = new BufferedReader(new FileReader(new File("E:\\mdw_data\\complete\\tb_result.txt")));
        HashMap<Long, Double> resutltMap = new HashMap<>();
        String result_str = tb_br.readLine();
        while (result_str!=null) {
            String[] result = result_str.split(",");
            resutltMap.put(Long.valueOf(result[0]), Double.valueOf(result[1]));
        }

        Set<Map.Entry<Long, Double>> tbEntrySet = resutltMap.entrySet();
        HashMap<Long, String> tbResultMap = new HashMap<>();
        int tbEntrySetSize = tbEntrySet.size();
        float tbSuccess = 0;
        for (Map.Entry entry : tbEntrySet) {
            long timeStamp = (long) entry.getKey();
            double tairTaobaoPrice = tairOperator.get(RaceConfig.prex_taobao+timeStamp) == null ? 0.0 :(double)  tairOperator.get(RaceConfig.prex_taobao+timeStamp) ;

            String str;
            if (tairTaobaoPrice == (double) entry.getValue()) {
                tbSuccess++;
                str = RaceConfig.prex_taobao+timeStamp + ", Result: Success. Amount is " + String.valueOf(tairTaobaoPrice) + "\n";
            } else {
                str = RaceConfig.prex_taobao+timeStamp + ", Result: Failed. Tair: " + tairTaobaoPrice +
                                                                ", Producer: "+ entry.getValue() + "\n";
            }
            tbResultMap.put(timeStamp,str);
            bw.write(str);
        }
        bw.write("Taobao准确率: " + tbSuccess/tbEntrySetSize + "\n");
        bw.flush();
        bw.close();
        System.out.println("Taobao准确率: " + tbSuccess/tbEntrySetSize);
    }

    public void analyseTmall() throws IOException, InterruptedException {
        Thread.sleep(5000);
        BufferedWriter tm_bw_result = new BufferedWriter(new FileWriter(new File("E:\\mdw_data\\tm_result.txt")));
        BufferedReader tb_br = new BufferedReader(new FileReader(new File("E:\\mdw_data\\complete\\tm_result.txt")));
        HashMap<Long, Double> resutltMap = new HashMap<>();
        String result_str = tb_br.readLine();
        while (result_str!=null) {
            String[] result = result_str.split(",");
            resutltMap.put(Long.valueOf(result[0]), Double.valueOf(result[1]));
        }

        Set<Map.Entry<Long, Double>> tmEntrySet = resutltMap.entrySet();
        HashMap<Long, String> tmResultMap = new HashMap<>();
        int tmEntrySetSize = tmEntrySet.size();
        float tmSuccess = 0;
        for (Map.Entry entry : tmEntrySet) {
            long timeStamp = (long) entry.getKey();
            double tairTaobaoPrice = tairOperator.get(RaceConfig.prex_taobao+timeStamp) == null ? 0.0 :(double)  tairOperator.get(RaceConfig.prex_taobao+timeStamp) ;

            String str;
            if (tairTaobaoPrice == (double) entry.getValue()) {
                tmSuccess++;
                str = RaceConfig.prex_tmall+timeStamp + ", Result: Success. Amount is " + String.valueOf(tairTaobaoPrice) + "\n";
            } else {
                str = RaceConfig.prex_tmall+timeStamp + ", Result: Failed. Tair: " + tairTaobaoPrice +
                        ", Producer: "+ entry.getValue() + "\n";
            }
            tmResultMap.put(timeStamp,str);
            bw.write(str);
        }
        bw.write("Tmall准确率: " + tmSuccess/tmEntrySetSize + "\n");
        bw.flush();
        bw.close();
        System.out.println("Tmall准确率: " + tmSuccess/tmEntrySetSize);
    }

    public void analysePayment() throws IOException, InterruptedException {
        Thread.sleep(5000);
        BufferedWriter py_bw_result = new BufferedWriter(new FileWriter(new File("E:\\mdw_data\\py_result.txt")));
        BufferedReader tb_br = new BufferedReader(new FileReader(new File("E:\\mdw_data\\complete\\py_result.txt")));
        HashMap<Long, double[]> resutltMap = new HashMap<>();
        String result_str = tb_br.readLine();
        while (result_str!=null) {
            String[] result = result_str.split(",");
            resutltMap.put(Long.valueOf(result[0]), new double[]{Double.valueOf(result[1]),Double.valueOf(result[2])});
        }

        double pcTotalPrice = 0;
        double moTotalPrice = 0;
        Set<Map.Entry<Long, double[]>> EntrySet = resutltMap.entrySet();
        HashMap<Long, String> ResultMap = new HashMap<>();
        int EntrySetSize = EntrySet.size();
        float success = 0;
        for (Map.Entry<Long, double[]> entry : EntrySet) {
            long timeStamp = entry.getKey();
            double tairRatio = tairOperator.get(RaceConfig.prex_ratio+timeStamp) == null ? 0.0 : (double) tairOperator.get(RaceConfig.prex_ratio+timeStamp);
            pcTotalPrice += entry.getValue()[0];
            moTotalPrice += entry.getValue()[1];
            double ratio = moTotalPrice/pcTotalPrice;
            String str;
            if (tairRatio == ratio) {
                success++;
                str = RaceConfig.prex_ratio+timeStamp + ", Result: Success. Ratio is " + tairRatio + "\n";
            } else {
                str = RaceConfig.prex_ratio+timeStamp + ", Result: Failed. Tair: " + tairRatio +
                        ", Producer: "+ ratio + "\n";
            }
            ResultMap.put(timeStamp,str);
            bw.write(str);
        }
        bw.write("支付信息准确率: " + success/EntrySetSize + "\n");
        bw.flush();
        bw.close();
        System.out.println("支付信息准确率: " + success/EntrySetSize);
    }

    public void startTime(){
        if (startTime == 0)
            startTime = System.currentTimeMillis();
    }

}
