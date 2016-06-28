package com.alibaba.middleware.race.Tair;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangwenfeng on 5/25/16.
 */
public class TairOperator {
    public static void main(String[] args) {
        List<String> list = new ArrayList<String>();
        list.add("192.168.1.161:5198");
        TairOperatorImpl tairOperator = new TairOperatorImpl(list);
        tairOperator.write(111, "eddfaef");
//        System.out.println(tairOperator.get("abc"));
        System.out.println(tairOperator.get(111));
    }
}
