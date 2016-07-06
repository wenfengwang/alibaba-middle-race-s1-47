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
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by sxian.wang on 2016/7/5.
 */
public class FileProducer {
    private static Random rand = new Random();
    private static int count = 2000;
    private static final Object lockObj = new Object();
    private static DefaultMQProducer producer;
    public static AtomicInteger atomicInteger = new AtomicInteger(0);
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
            atomicInteger.addAndGet(1);
        }
    }

    public void produceOrder(BufferedReader br, int platform)
            throws IOException, RemotingException, MQClientException, InterruptedException, MQBrokerException {
        String str = br.readLine();
        final String [] topics = new String[]{RaceConfig.MqTaobaoTradeTopic, RaceConfig.MqTmallTradeTopic};


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
            str = br.readLine();
        }
        byte [] zero = new  byte[]{0,0};
        Message endMsg = new Message(topics[platform], zero);
        producer.send(endMsg);
    }

    public void producePayment() throws IOException, RemotingException, MQClientException, InterruptedException, MQBrokerException {
//        BufferedReader py_br_data = new BufferedReader(new FileReader(new File("E:\\mdw_data\\complete\\py_data.txt")));
        BufferedReader py_br_data = new BufferedReader(new FileReader(new File("E:\\mdw_data\\py_data.txt")));
        String str = py_br_data.readLine();

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
            str = py_br_data.readLine();
        }
        byte [] zero = new  byte[]{0,0};
        Message endMsg = new Message(RaceConfig.MqPayTopic, zero);
        producer.send(endMsg);
    }
    public static void main(String[] args)
            throws MQClientException, InterruptedException, IOException, RemotingException, MQBrokerException {
        int count = 0;
        while (count++ < 3) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    FileProducer fp = new FileProducer();
                    try {

                        switch (fp.atomicInteger.get()) {
                            case 1:
                                fp.producePayment();
                                break;
                            case 2:
//                                BufferedReader tb_br = new BufferedReader(new FileReader(new File("E:\\mdw_data\\complete\\tb_data.txt")));
                                BufferedReader tb_br = new BufferedReader(new FileReader(new File("E:\\mdw_data\\tb_data.txt")));
                                fp.produceOrder(tb_br,0);
                                break;
                            case 3:
//                                BufferedReader tm_br = new BufferedReader(new FileReader(new File("E:\\mdw_data\\complete\\tm_data.txt")));
                                BufferedReader tm_br = new BufferedReader(new FileReader(new File("E:\\mdw_data\\tm_data.txt")));
                                fp.produceOrder(tm_br,1);
                                break;
                            default:
                                break;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
            thread.start();
        }
    }
}
