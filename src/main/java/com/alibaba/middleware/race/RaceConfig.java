package com.alibaba.middleware.race;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class RaceConfig implements Serializable {
    public static final String TeamCode = "373058h4iq";

    public static final boolean ONLINE = true;

    // jstorm
    public static final String JstormTopologyName = "373058h4iq";

    // Rocketmq
    public static final String MQNameServerAddr = "192.168.1.161:9876";
    public static final String MqConsumerGroup = "373058h4iq";
    public static final String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static final String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static final String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";

    // Tair
    public static final String TairGroup = "group_tianchi";
    public static final String prex_tmall = "platformTmall_" + TeamCode + "_";
    public static final String prex_taobao = "platformTaobao_" + TeamCode + "_";
    public static final String prex_ratio = "ratio_" + TeamCode + "_";

    //    public static final int TairNamespace = 19542;
//    public static final List<String> TairServerAddr = Arrays.asList(new String[]{"10.101.72.127:5198","10.101.72.129:5198"});
    public static final List<String> TairServerAddr = Arrays.asList(new String[]{"192.168.1.161:5198"});
    public static final int  TairNamespace = 1;


//    result path

    public static final String TM_LOG_PATH = "E:\\mdw_log\\Tm_";
    public static final String TB_LOG_PATH = "E:\\mdw_log\\Tb_";
    public static final String PY_LOG_PATH = "E:\\mdw_log\\Py_";

    public static final String CREATE_DATA_PREFIX = "E:\\mdw_data\\30min\\";
    public static final String FILE_PRODUCER_SOURCE_PREFIX = "E:\\mdw_data\\";

//    public static final String TB_LOG_PATH = "/home/dev/mdw_log/Tb_";
//    public static final String TM_LOG_PATH = "/home/dev/mdw_log/Tm_";
//    public static final String PY_LOG_PATH = "/home/dev/mdw_log/Py_";

    // ckeck duplicated
    public static final boolean CHECK_ORDER_DUPLICATED = false;
    public static final boolean CHECK_PAYMENT_DUPLICATED = false;

    // Topology config
    public static final String SPOUT_NAME = "BigSpout";
    public static final String TAOBAO_STREAM_ID = "TaoBao_Stream";
    public static final String TMALL_STREAM_ID = "Tmall_Stream";

    public static final String PAYMENT_STREAM_ID = "Payment_Stream";
    public static final String TAOBAO_COUNT_BOLT_ID = "CountTaobao";
    public static final String TAOBAO_PERSIST_BOLT_ID = "PerisistTaobao";
    public static final String TMALL_COUNT_BOLT_ID = "CountTmall";
    public static final String TMALL_PERSIST_BOLT_ID = "PerisistTmall";
    public static final String RATIO_SPLIT_BOLT_ID = "splitPayment";
    public static final String RATIO_COUNT_BOLT_ID = "CountPayment";


    public static final String RATIO_PERSIST_BOLT_ID = "PersistRatio";
}
