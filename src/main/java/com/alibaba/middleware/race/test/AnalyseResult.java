package com.alibaba.middleware.race.test;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.jar.Pack200;

/**
 * Created by wangwenfeng on 7/4/16.
 */
public class AnalyseResult {

    private final TairOperatorImpl tairOperator = new TairOperatorImpl();
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
        Thread.currentThread().setName("analyseTaobao");
        Thread.sleep(5000);
        BufferedReader tb_br = new BufferedReader(new FileReader(new File(RaceConfig.FILE_PRODUCER_SOURCE_PREFIX + "tb_result.txt")));
        HashMap<Long, Double> resutltMap = new HashMap<>();
        String result_str = tb_br.readLine();
        while (result_str!=null) {
            String[] result = result_str.split(",");
            resutltMap.put(Long.valueOf(result[0]), Double.valueOf(result[1]));
            result_str = tb_br.readLine();
        }

        Set<Map.Entry<Long, Double>> tbEntrySet = resutltMap.entrySet();
        int tbEntrySetSize = tbEntrySet.size();
        float tbSuccess = 0;
        try {

        for (Map.Entry<Long, Double> entry : tbEntrySet) {
            long timeStamp = entry.getKey();
            Double tairTaobaoPrice = (Double) tairOperator.get(RaceConfig.prex_taobao+timeStamp);
            tairTaobaoPrice = tairTaobaoPrice == null ? 0.0 : tairTaobaoPrice ;

            String str;
            if (tairTaobaoPrice > entry.getValue()-1 && tairTaobaoPrice < entry.getValue()+1) {
                tbSuccess++;
                str = RaceConfig.prex_taobao+timeStamp + ", Result: Success. Tair: " + RaceUtils.round(tairTaobaoPrice) +
                                                                ", Producer: "+ RaceUtils.round(entry.getValue()) + "\n";
            } else {
                str = RaceConfig.prex_taobao+timeStamp + ", Result: Failed. Tair: " + RaceUtils.round(tairTaobaoPrice) +
                                                                ", Producer: "+ RaceUtils.round(entry.getValue()) + "\n";
            }
            bw.write(str);
        }
        } catch (Exception e) {
            e.printStackTrace();
        }
        bw.write("Taobao准确率: " + tbSuccess/tbEntrySetSize + "\n");
        bw.flush();
        bw.close();
    }

    public void analyseTmall() throws IOException, InterruptedException {
        Thread.currentThread().setName("analyseTmall");
        Thread.sleep(5000);
        BufferedReader tm_br = new BufferedReader(new FileReader(new File(RaceConfig.FILE_PRODUCER_SOURCE_PREFIX + "tm_result.txt")));
        HashMap<Long, Double> resutltMap = new HashMap<>();
        String result_str = tm_br.readLine();
        while (result_str!=null) {
            String[] result = result_str.split(",");
            resutltMap.put(Long.valueOf(result[0]), Double.valueOf(result[1]));
            result_str = tm_br.readLine();
        }

        Set<Map.Entry<Long, Double>> tmEntrySet = resutltMap.entrySet();
        int tmEntrySetSize = tmEntrySet.size();
        float tmSuccess = 0;
        try {

        for (Map.Entry<Long, Double> entry : tmEntrySet) {
            long timeStamp = entry.getKey();
            Double tairTmallPrice = (Double) tairOperator.get(RaceConfig.prex_tmall+timeStamp);
            tairTmallPrice = tairTmallPrice == null ? 0.0 : tairTmallPrice ;

            String str;
            if (tairTmallPrice > entry.getValue()-1 && tairTmallPrice <= entry.getValue()+1) {
                tmSuccess++;
                str = RaceConfig.prex_tmall+timeStamp + ", Result: Success. Tair: " + RaceUtils.round(tairTmallPrice) +
                                                            ", Producer: "+ RaceUtils.round(entry.getValue()) + "\n";
            } else {
                str = RaceConfig.prex_tmall+timeStamp + ", Result: Failed. Tair: " + RaceUtils.round(tairTmallPrice) +
                                                            ", Producer: "+ RaceUtils.round(entry.getValue()) + "\n";
            }
            bw.write(str);
        }
        } catch (Exception e) {
            e.printStackTrace();
        }
        bw.write("Tmall准确率: " + tmSuccess/tmEntrySetSize + "\n");
        bw.flush();
        bw.close();
    }

    public void analysePayment(String name) throws IOException, InterruptedException {
        Thread.currentThread().setName("analysePayment");
        Thread.sleep(5000);
        BufferedReader py_br = new BufferedReader(new FileReader(new File(RaceConfig.FILE_PRODUCER_SOURCE_PREFIX + name +".txt")));
        TreeMap<Long, double[]> resutltMap = new TreeMap<>();
        String result_str = py_br.readLine();
        while (result_str!=null) {
            String[] result = result_str.split(",");
            resutltMap.put(Long.valueOf(result[0]), new double[]{Double.valueOf(result[1]),Double.valueOf(result[2])});
            result_str = py_br.readLine();
        }

        double pcTotalPrice = 0;
        double moTotalPrice = 0;
        Set<Map.Entry<Long, double[]>> EntrySet = resutltMap.entrySet();
        TreeMap<Long, String> ResultMap = new TreeMap<>();
        int EntrySetSize = EntrySet.size();
        float success = 0;
        for (Map.Entry<Long, double[]> entry : EntrySet) {
            long timeStamp = entry.getKey();
            double tairRatio = tairOperator.get(RaceConfig.prex_ratio+timeStamp) == null ? 0.0 : (double) tairOperator.get(RaceConfig.prex_ratio+timeStamp);
            pcTotalPrice += entry.getValue()[0];
            moTotalPrice += entry.getValue()[1];
            double ratio = moTotalPrice/pcTotalPrice;
            String str;
            if (tairRatio > ratio-0.01 && tairRatio < ratio+0.01) {
                success++;
                str = RaceConfig.prex_ratio+timeStamp + ", Result: Success. Tair: " + RaceUtils.round(tairRatio) +
                                                            ", Producer: "+ RaceUtils.round(ratio) + "\n";
            } else {
                str = RaceConfig.prex_ratio+timeStamp + ", Result: Failed. Tair: " + RaceUtils.round(tairRatio) +
                                                            ", Producer: "+ RaceUtils.round(ratio) + "\n";
            }
            ResultMap.put(timeStamp,str);
            bw.write(str);
        }
        bw.write("支付信息准确率: " + success/EntrySetSize + "\n");
        bw.flush();
        bw.close();
    }

    public void startTime(){
        if (startTime == 0)
            startTime = System.currentTimeMillis();
    }

}
