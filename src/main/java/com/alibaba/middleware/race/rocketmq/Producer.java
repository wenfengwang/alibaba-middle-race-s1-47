package com.alibaba.middleware.race.rocketmq;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.test.AnalyseResult;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.middleware.race.model.*;
import com.alibaba.middleware.race.RaceUtils;


import java.io.*;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;


public class Producer {

    private static Random rand = new Random();
    private static int count = 200000000;
    private static AtomicInteger atomIntTb = new AtomicInteger(0);
    private static AtomicInteger atomIntTm = new AtomicInteger(0);
    private static AtomicInteger atomIntPy = new AtomicInteger(0);

    public static void main(String[] args) throws MQClientException, InterruptedException, IOException {
        DefaultMQProducer producer = new DefaultMQProducer(RaceConfig.MqConsumerGroup);
        producer.setNamesrvAddr("192.168.1.161:9876");
        producer.start();
        AnalyseResult analyseResult = new AnalyseResult();
        final String [] topics = new String[]{RaceConfig.MqTaobaoTradeTopic, RaceConfig.MqTmallTradeTopic};
        final Semaphore semaphore = new Semaphore(0);

        for (int i = 0; i < count; i++) {
            try {
                final int platform = rand.nextInt(2);
                if (platform == 0) {
                    atomIntTb.addAndGet(1);
                } else {
                    atomIntTm.addAndGet(1);
                }
                final OrderMessage orderMessage = ( platform == 0 ? OrderMessage.createTbaoMessage() : OrderMessage.createTmallMessage());
                orderMessage.setCreateTime(System.currentTimeMillis());
//                analyseResult.addOrder(orderMessage, platform);
                byte [] body = RaceUtils.writeKryoObject(orderMessage);
                Message msgToBroker = new Message(topics[platform], body);
                producer.send(msgToBroker, new SendCallback() {
                    public void onSuccess(SendResult sendResult) {

//                        System.out.println(orderMessage);
                        semaphore.release();
                    }
                    public void onException(Throwable throwable) {
                        throwable.printStackTrace();
                    }
                });

                //Send Pay message
                PaymentMessage[] paymentMessages = PaymentMessage.createPayMentMsg(orderMessage);
                double amount = 0;
                for (final PaymentMessage paymentMessage : paymentMessages) {
                    atomIntPy.addAndGet(1);
                    int retVal = Double.compare(paymentMessage.getPayAmount(), 0);
                    if (retVal < 0) {
                        throw new RuntimeException("price < 0 !!!!!!!!");
                    }

                    if (retVal > 0) {
                        amount += paymentMessage.getPayAmount();
//                        analyseResult.addPayment(paymentMessage);
                        final Message messageToBroker = new Message(RaceConfig.MqPayTopic, RaceUtils.writeKryoObject(paymentMessage));
                        producer.send(messageToBroker, new SendCallback() {
                            public void onSuccess(SendResult sendResult) {

//                                System.out.println(paymentMessage);
                            }
                            public void onException(Throwable throwable) {
                                throwable.printStackTrace();
                            }
                        });
                    }
                }

                if (Double.compare(amount, orderMessage.getTotalPrice()) != 0) {
                    throw new RuntimeException("totalprice is not equal.");
                }


            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
//            Thread.sleep(1000);
        }
        semaphore.acquire(count);

        byte [] zero = new  byte[]{0,0};
        Message endMsgTB = new Message(RaceConfig.MqTaobaoTradeTopic, zero);
        Message endMsgTM = new Message(RaceConfig.MqTmallTradeTopic, zero);
        Message endMsgPay = new Message(RaceConfig.MqPayTopic, zero);

        try {
            System.out.println("TB: "+atomIntTb);
            System.out.println("TM: "+atomIntTm);
            System.out.println("PY: "+atomIntPy);

            producer.send(endMsgTB);
            producer.send(endMsgTM);
            producer.send(endMsgPay);

        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.shutdown();
    }
}
