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
        Random random = new Random(47);
        long time = System.currentTimeMillis();
        HashSet<Long> testSet = new HashSet<>();
        for (int i = 0;i<10000000;i++) {
            testSet.add(random.nextLong());
        }

        long midtime = System.currentTimeMillis();
        System.out.println(midtime - time);
        for (int i = 0;i<1000000;i++) {
            long l = random.nextInt(200000);
            boolean finded = testSet.contains(l);
        }
        System.out.println("total time: " +(System.currentTimeMillis()-midtime));
    }

}
