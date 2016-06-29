package com.alibaba.middleware.race.Tair;

import com.alibaba.middleware.race.RaceConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wangwenfeng on 5/25/16.
 */
public class TairOperator {
    public static void main(String[] args) {
        ArrayList<String> list = new ArrayList<String>();
        list.add("192.168.1.161:5198");
        TairOperatorImpl tairOperator = new TairOperatorImpl(list);
//        tairOperator.write("asd","123");
        System.out.println(tairOperator.get(RaceConfig.prex_taobao+"1467191460"));
    }
}
