package com.alibaba.middleware.race.test;

import com.alibaba.middleware.race.RaceConfig;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by sxian.wang on 2016/7/6.
 */
public class AnalyseThread implements Runnable {
    private AnalyseResult analyseResult;
    private final int type;

    public AnalyseThread(String path, int type) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
        if (!RaceConfig.ONLINE) {
            String filePath = path + sdf.format(new Date(System.currentTimeMillis()));
            analyseResult = new AnalyseResult(filePath+".log");
        }
        this.type = type;
    }

    public void run() {
        try {
            switch (type) {
                case 1:
                    analyseResult.analyseTaobao();
                    break;
                case 2:
                    analyseResult.analyseTmall();
                    break;
                case 3:
                    analyseResult.analysePayment("py_result");
                    break;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
