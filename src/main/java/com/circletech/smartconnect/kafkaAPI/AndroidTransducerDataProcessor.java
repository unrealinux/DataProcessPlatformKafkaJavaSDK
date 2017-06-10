package com.circletech.smartconnect.kafkaAPI;

/**
 * Created by Administrator on 2016/12/19.
 */
public class AndroidTransducerDataProcessor implements TransducerDataProcessor {

    public void newTransducerData(TransportCommData transportTransducerData) {
        System.out.print(transportTransducerData.toString());
    }

    public void newData(String data){
        System.out.print(data);
    }
}
