package com.example.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author: BYDylan
 * @date: 2023/11/05
 * @description:
 */
public class CustomProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        // kafka 集群// ,broker-list
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.101:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // 重试次数
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        // 批次大小
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // 等待时间
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        // RecordAccumulator 缓冲区大小
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++) {
            // try {
            //// 不带回调函数
            // producer.send(new ProducerRecord<String, String>("first", Integer.toString(i),
            // Integer.toString(i))).get();
            // } catch (InterruptedException | ExecutionException e) {
            // e.printStackTrace();
            // }
            // 回调函数// ,该方法会在 Producer 收到 ack 时调用// ,为异步调用
            producer.send(new ProducerRecord<>("first", Integer.toString(i), Integer.toString(i)),
                (metadata, exception) -> {
                    if (exception == null) {
                        System.out.println("success->" + metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                });
        }
        producer.close();
    }

}
