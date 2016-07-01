package com.alibaba.middleware.race;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RaceConfig implements Serializable {
    public static final String TeamCode = "373058h4iq";

    // jstorm
    public static final String JstormTopologyName = "373058h4iq";

    // Rocketmq
    public static final String MQNameServerAddr = "192.168.1.161:9876";
    public static final String MqConsumerGroup = "AliMiddleWareRace";
    public static final String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static final String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static final String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";

    // Tair
    public static final String TairGroup = "group_tianchi";
    public static final String prex_tmall = "platformTmall_" + TeamCode + "_";
    public static final String prex_taobao = "platformTaobao_" + TeamCode + "_";
    public static final String prex_ratio = "ratio_" + TeamCode + "_";

    // online
    public static final int OnLineNamespace = 19542;
    public static final List<String> OnLineTairServerAddr = Arrays.asList(new String[]{"10.101.72.127:5198","10.101.72.129:5198"});
    // online
    public static final List<String> OffLineTairServerAddr = Arrays.asList(new String[]{"192.168.1.161:5198"});
    public static final int  OffLineTairNamespace = 1;
}
