package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {
    private static String TeadCode = "373058h4iq";

    // jstorm
    public static String JstormTopologyName = "373058h4iq";

    // Rocketmq
    public static String MQCounsumerConfig = "373058h4iq";
    public static String MQNameServerAddr = "192.168.1.161:9876";
    public static String MqConsumerGroup = "AliMiddleWareRace";
    public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";

    // Tair
    public static String TairGroup = "group_tianchi";
    public static Integer TairNamespace = 1;
    public static String prex_tmall = "platformTmall_" + TeadCode + "_";
    public static String prex_taobao = "platformTaobao_" + TeadCode + "_";
    public static String prex_ratio = "ratio_" + TeadCode + "_";
}
