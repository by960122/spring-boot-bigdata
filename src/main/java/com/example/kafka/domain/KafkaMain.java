package com.example.kafka.domain;

import com.example.kafka.consumer.KafkaConsumerThread;
import com.example.kafka.producer.KafkaProducerThread;

public class KafkaMain {

    public static void main(String[] args) {
        // 使用sasl与kafka集群通讯之前记得将jaas文件加入到jvm，详见文档
        // System.setProperty("java.security.auth.login.config", "D:\\WorkSpace\\ideaProject\\kafka_client_jaas.conf");
        // consume();
        produce();
    }

    private static void consume() {
        // 为了达到最大吞吐量，建议有多少个分区就建多少条线程,Topic可以写多个
        KafkaConsumerThread consumer = new KafkaConsumerThread("bingo", "by201:9092,by211:9092,by212:9092", "bingo");
        consumer.start();
    }

    private static void produce() {
        KafkaProducerThread producer = new KafkaProducerThread("bingo", "by201:9092,by211:9092,by212:9092");
        producer.start();
    }
}
