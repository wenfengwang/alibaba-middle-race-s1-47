package com.alibaba.middleware.race.Tair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

/**
 * Created by wangwenfeng on 5/25/16.
 */
public class TairOperator {
    public static void main(String[] args) {

//
       HashSet<Long> hs = new HashSet<>();
        long a = 123l;
        long b = 123165l;
        long c = 1234567891012l;
        Long d = 1234567891012l;
        hs.add(a);
        hs.add(b);
        hs.add(c);
        Object[] objects = new Object[] {a,b};
        System.out.println(hs.contains(objects[0]));
        System.out.println(hs.contains(objects[1]));
        System.out.println(hs.contains(d));

    }

}
