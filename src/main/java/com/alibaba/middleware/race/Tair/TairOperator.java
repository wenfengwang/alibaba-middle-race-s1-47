package com.alibaba.middleware.race.Tair;

import com.alibaba.middleware.race.RaceConfig;

/**
 * Created by wangwenfeng on 5/25/16.
 */
public class TairOperator {
    public static void main(String[] args) {

        TairOperatorImpl tairOperator = new TairOperatorImpl();
        System.out.println("Result: "+tairOperator.get("platformTaobao_373058h4iq_1467960540"));
        System.out.println("Result: "+tairOperator.get("platformTaobao_373058h4iq_1467960600"));
        System.out.println("Result: "+tairOperator.get("platformTaobao_373058h4iq_1467960480"));
        System.out.println("Result: "+tairOperator.get("platformTaobao_373058h4iq_1467960420"));

    }

}
