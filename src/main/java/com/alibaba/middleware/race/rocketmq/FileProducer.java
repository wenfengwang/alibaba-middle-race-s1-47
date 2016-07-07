package com.alibaba.middleware.race.rocketmq;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by sxian.wang on 2016/7/5.
 */
public class FileProducer {
    private static final Object lockObj = new Object();
    private static DefaultMQProducer producer;

    public FileProducer() {
        synchronized (lockObj) {
            if (producer == null) {
                producer = new DefaultMQProducer(RaceConfig.MqConsumerGroup);
                producer.setNamesrvAddr("192.168.1.161:9876");
                try {
                    producer.start();
                } catch (MQClientException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void produceOrder(BufferedReader br, int platform)
            throws IOException, RemotingException, MQClientException, InterruptedException, MQBrokerException {
        String str = br.readLine();
        final String [] topics = new String[]{RaceConfig.MqTaobaoTradeTopic, RaceConfig.MqTmallTradeTopic};
        int count = 0;
        double amount = 0.0;
        while (str != null) {
            String[] fileds = str.split(", ");

            OrderMessage orderMessage = new OrderMessage();
            orderMessage.setOrderId(Long.valueOf(fileds[0].split("=")[1]));
            orderMessage.setBuyerId(fileds[1].split("'")[1]);
            orderMessage.setProductId(fileds[2].split("'")[1]);
            orderMessage.setSalerId(fileds[3].split("'")[1]);
            orderMessage.setCreateTime(Long.valueOf(fileds[4].split("=")[1]));
            orderMessage.setTotalPrice(Double.valueOf(fileds[5].split("=")[1]));

            byte [] body = RaceUtils.writeKryoObject(orderMessage);
            Message msgToBroker = new Message(topics[platform], body);
            producer.send(msgToBroker);
//            producer.send(msgToBroker, new SendCallback() {
//                public void onSuccess(SendResult sendResult) {
//                    System.out.println("saf");
//                }
//                public void onException(Throwable throwable) {
//                    throwable.printStackTrace();
//                }
//            });
            count++;
            if (count %1000 == 0 )
                System.out.println(count);
            amount += orderMessage.getTotalPrice();
            str = br.readLine();
        }
        System.out.println("***** " + topics[platform] + " : " + count + " *****; " +amount);
        byte [] zero = new  byte[]{0,0};
        Message endMsg = new Message(topics[platform], zero);
        producer.send(endMsg);
    }

    public void producePayment() throws IOException, RemotingException, MQClientException, InterruptedException, MQBrokerException {
        BufferedReader py_br_data = new BufferedReader(new FileReader(new File(RaceConfig.FILE_PRODUCER_SOURCE_PREFIX + "py_data.txt")));
        String str = py_br_data.readLine();

        int count = 0;
        while (str != null) {
            String[] fileds = str.split(", ");

            PaymentMessage paymentMessage = new PaymentMessage();
            paymentMessage.setOrderId(Long.valueOf(fileds[0].split("=")[1]));
            paymentMessage.setPayAmount(Double.valueOf(fileds[1].split("=")[1]));
            paymentMessage.setPaySource(Short.valueOf(fileds[2].split("=")[1]));
            paymentMessage.setPlatForm(Short.valueOf(fileds[3].split("=")[1]));
            paymentMessage.setCreateTime(Long.valueOf(fileds[4].split("=")[1]));

            final Message messageToBroker = new Message(RaceConfig.MqPayTopic, RaceUtils.writeKryoObject(paymentMessage));
            producer.send(messageToBroker);
//            producer.send(messageToBroker, new SendCallback() {
//                public void onSuccess(SendResult sendResult) {
////                                System.out.println(paymentMessage);
//                }
//                public void onException(Throwable throwable) {
//                    throwable.printStackTrace();
//                }
//            });
            count++;
            if (count %1000 == 0 )
                System.out.println(count);
            str = py_br_data.readLine();
        }
        System.out.println("***** " + RaceConfig.MqPayTopic + " : " + count + " *****");
        byte [] zero = new  byte[]{0,0};
        Message endMsg = new Message(RaceConfig.MqPayTopic, zero);
        producer.send(endMsg);
    }
    public static void main(String[] args)
            throws MQClientException, InterruptedException, IOException, RemotingException, MQBrokerException {
        int count = 0;
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                FileProducer fp = new FileProducer();
                fp.producePayment();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    FileProducer fp = new FileProducer();
                    BufferedReader tb_br = new BufferedReader(new FileReader(new File(RaceConfig.FILE_PRODUCER_SOURCE_PREFIX + "tb_data.txt")));
                    fp.produceOrder(tb_br,0);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    FileProducer fp = new FileProducer();
                    BufferedReader tm_br = new BufferedReader(new FileReader(new File(RaceConfig.FILE_PRODUCER_SOURCE_PREFIX + "tm_data.txt")));
                    fp.produceOrder(tm_br,1);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}
