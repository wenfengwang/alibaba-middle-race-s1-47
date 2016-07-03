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
        int a,b;
        a = b = 2;
        System.out.println(a+" "+b);
        TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairServerAddr,RaceConfig.TairNamespace);
        System.out.println("*************************");
        System.out.println("Result: "+tairOperator.get("platformTmall_373058h4iq_1467532980"));
    }
}
