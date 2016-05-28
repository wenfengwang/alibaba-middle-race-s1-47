package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {

    //这些是写tair key的前缀
    public static String prex_tmall = "platformTmall_";
    public static String prex_taobao = "platformTaobao_";
    public static String prex_ratio = "ratio_";


    // jstorm
    public static String JstormTopologyName = "AlimiddleWareRace";
    public static String JstormNimbusAddr = "192.168.1.101";
    // zookeeper
    public static String ZookeeperService = "192.168.1.101";
    // Rocketmq
    public static String MQNameServerAddr = "192.168.1.101:9876";
    public static String MetaConsumerGroup = "Ali_mdw";
    public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";

    // Tair
    public static String TairConfigServer = "192.168.1.101:5198";
    public static String TairGroup = "group_1";
    public static Integer TairNamespace = 1;
}
