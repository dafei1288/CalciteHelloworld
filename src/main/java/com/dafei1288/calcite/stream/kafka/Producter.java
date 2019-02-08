package com.dafei1288.calcite.stream.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class Producter {
    private static KafkaProducer<String, String> producer;
    //刚才构建的topic
    private final static String TOPIC = "calcitekafka";
    public Producter(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //设置分区类,根据key进行数据分区
        producer = new KafkaProducer<String, String>(props);
    }
    public void produce(){
        int i = 0;
        Random r = new Random();
        for(;;){
            //每一秒创建一个随机的布尔值
            producer.send(new ProducerRecord<String, String>(TOPIC,i+++"",r.nextBoolean()+"" ));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
       // producer.close();
    }

    public static void main(String[] args) {
        new Producter().produce();
    }
}
