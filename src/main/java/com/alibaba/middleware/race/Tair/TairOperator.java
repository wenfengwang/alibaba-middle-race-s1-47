package com.alibaba.middleware.race.Tair;

import com.alibaba.middleware.race.RaceConfig;

/**
 * Created by wangwenfeng on 5/25/16.
 */
public class TairOperator {
    public static void main(String[] args) {

        TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairServerAddr,RaceConfig.TairNamespace);
        System.out.println("Result: "+tairOperator.get("platformTaobao_373058h4iq_1467951960"));
        System.out.println("Result: "+tairOperator.get("platformTaobao_373058h4iq_1467959640"));
        System.out.println("Result: "+tairOperator.get("platformTaobao_373058h4iq_1467952980"));
        System.out.println("Result: "+tairOperator.get("platformTaobao_373058h4iq_1467946320"));

    }

}
