package com.alibaba.middleware.race.Tair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wangwenfeng on 5/25/16.
 */
public class TairOperator {
    public static void main(String[] args) {

        Map<String,Double> map = new ConcurrentHashMap<String,Double>();
        map.put("asd",2.2);
        try {
            String test = String.valueOf(map.get("12"));
            System.out.println(Double.valueOf(test));
        } catch (Exception e) {
            e.printStackTrace();
        }
//        List<String> list = new ArrayList<String>();
//        list.add("192.168.1.161:5198");
//        TairOperatorImpl tairOperator = new TairOperatorImpl(list);
//
////        tairOperator.write("a", "1");
//        System.out.println(tairOperator.get(String.valueOf("a")));
//        System.exit(0);
////        for (int i = 0;i<1000;i++) {
////            tairOperator.write("a"+i, i);
////        }
////        for (int i = 0;i<1000;i++) {
////            System.out.println(tairOperator.get(String.valueOf("a"+i)));
////        }
    }
}
