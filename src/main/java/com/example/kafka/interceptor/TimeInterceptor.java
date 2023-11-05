package com.example.kafka.interceptor;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author: BYDylan
 * @date: 2023/11/05
 * @description:
 */
public class TimeInterceptor implements ProducerInterceptor<String, String> {

    @Override
    public void configure(Map<String, ?> map) {}

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        // 创建一个新的 record,把时间戳写入消息体的最前部
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), record.key(),
            System.currentTimeMillis() + "," + record.value().toString());
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {}

    @Override
    public void close() {}
}
