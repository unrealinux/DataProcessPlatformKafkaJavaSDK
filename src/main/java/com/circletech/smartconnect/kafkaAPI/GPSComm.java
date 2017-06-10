package com.circletech.smartconnect.kafkaAPI;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by xieyingfei on 2016/12/17.
 */
public class GPSComm {

    private static KafkaProducer kafkaProducer;
    private static KafkaConsumerTransducer kafkaConsumerTransducer;

    //成功返回0，失败返回1
    public static int init(InputStream propReader){

        try {
            kafkaProducer = new KafkaProducer();

            Properties properties = new Properties();
            try {
                properties.load(propReader);
                propReader.close();
            }catch (IOException e){
                e.printStackTrace();
            }
            kafkaConsumerTransducer = new KafkaConsumerTransducer(properties);
        }catch (Exception e){
            e.printStackTrace();
            return 1;
        }

        return 0;
    }

    //成功返回0，失败返回1
    public static int open(String message, TransducerDataProcessor transducerDataProcessor){
        try
        {
            try {
                if(message.equals("android-open-sensor")){
                    if(transducerDataProcessor != null){
                        kafkaProducer.produce("android-open-sensor");
                        kafkaConsumerTransducer.setTransducerDataProcessor(transducerDataProcessor);

                        ExecutorService executorService = Executors.newCachedThreadPool();
                        executorService.submit(kafkaConsumerTransducer);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }catch (Exception e){
            e.printStackTrace();
            return 1;
        }

        return 0;
    }

    //成功返回0，失败返回1
    public static int close(String message){

        try {
            kafkaConsumerTransducer.stopConsume();
        }catch (Exception e){
            e.printStackTrace();
            return 1;
        }

        return 0;
    }
}
