package com.circletech.smartconnect.kafkaAPI;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Administrator on 2017/6/9.
 */
public class KafkaConsumerTransducer implements Runnable {

    private final ConsumerConnector consumer;
    static final String transducer_topic = "transducer";

    private boolean bStartConsume = false;
    private TransducerDataProcessor transducerDataProcessor;

    public TransducerDataProcessor getTransducerDataProcessor() {
        return transducerDataProcessor;
    }

    public void setTransducerDataProcessor(TransducerDataProcessor transducerDataProcessor) {
        this.transducerDataProcessor = transducerDataProcessor;
    }

    public KafkaConsumerTransducer(Properties properties) {
        ConsumerConfig config = new ConsumerConfig(properties);

        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);

        bStartConsume = true;
    }

    @Override
    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(transducer_topic, new Integer(1));

        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

        Map<String, List<KafkaStream<String, String>>> consumerMap =
                consumer.createMessageStreams(topicCountMap,keyDecoder,valueDecoder);
        KafkaStream<String, String> stream = consumerMap.get(transducer_topic).get(0);
        ConsumerIterator<String, String> it = stream.iterator();
        while (it.hasNext() && bStartConsume){
            transducerDataProcessor.newData(it.next().message());

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void stopConsume(){
        bStartConsume = false;
    }
}
