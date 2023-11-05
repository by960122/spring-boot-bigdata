package com.example.kafka.producer;

import java.util.ArrayList;
import java.util.List;
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
public class InterceptorProducer {
    public static void main(String[] args) {
        // 1.设置配置信息
        Properties props = new Properties();
        // kafka 集群，broker-list
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
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
        // 2.构建拦截链
        List<String> interceptors = new ArrayList<>();
        interceptors.add("interceptor.CounterInterceptor ptor");
        interceptors.add("interceptor.TimeInterceptor rceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
        String topic = "first";
        Producer<String, String> producer = new KafkaProducer<>(props);
        // 3.发送消息
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, "message" + i);
            producer.send(record);
        }
        // 4.一定要关闭 producer，这样才会调用 interceptor 的 close 方法
        producer.close();
    }
}
