package com.alibaba.middleware.race.test;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.PaymentMessage;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by sxian.wang on 2016/7/8.
 */
public class ConfirmResult {
    public static void confirmPayment() throws IOException {
        BufferedReader py_br = new BufferedReader(new FileReader(new File(RaceConfig.FILE_PRODUCER_SOURCE_PREFIX + "py_data.txt")));
        BufferedWriter py_confrim = new BufferedWriter(new FileWriter(new File(RaceConfig.FILE_PRODUCER_SOURCE_PREFIX + "py_result_confirm.txt")));

        String str = py_br.readLine();
        HashMap<Long, double[]> map = new HashMap<>();
        int count = 0;
        while (str != null) {
            String[] fileds = str.split(", ");

            PaymentMessage paymentMessage = new PaymentMessage();
            paymentMessage.setOrderId(Long.valueOf(fileds[0].split("=")[1]));
            paymentMessage.setPayAmount(Double.valueOf(fileds[1].split("=")[1]));
            paymentMessage.setPaySource(Short.valueOf(fileds[2].split("=")[1]));
            paymentMessage.setPlatForm(Short.valueOf(fileds[3].split("=")[1]));
            paymentMessage.setCreateTime(Long.valueOf(fileds[4].split("=")[1]));

            long timeStamp = RaceUtils.toMinuteTimeStamp(paymentMessage.getCreateTime());

            double[] amount = map.get(timeStamp);
            if (amount == null) {
                amount = new double[]{0,0};
            }

            amount[paymentMessage.getPayPlatform()] += paymentMessage.getPayAmount();
            map.put(timeStamp,amount);
            str = py_br.readLine();
            if (count++%10000 == 0)
                System.out.println(count-1);
        }

        Set<Map.Entry<Long, double[]>> mapEntrySet = map.entrySet();

        for (Map.Entry<Long, double[]> entry : mapEntrySet) {
            py_confrim.write(entry.getKey()+","+entry.getValue()[0]+","+entry.getValue()[1]+","+entry.getValue()[1]/entry.getValue()[0]+"\n");
        }
        py_confrim.flush();
        py_confrim.close();
    }

    public static void main(String[] args) {
        try {
            confirmPayment();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
