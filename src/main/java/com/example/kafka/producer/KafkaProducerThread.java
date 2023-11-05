package com.example.kafka.producer;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;

/***
 * @author: BYDylan
 * @date: 2023/11/5
 * @description: Kafka生产线程
 */
public class KafkaProducerThread extends Thread {
    private final KafkaProducer<String, String> producer;
    private final String topic;

    public KafkaProducerThread(String topic, String brokerList) {
        // 指定kafka节点
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        // props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        // 需要server接收到数据之后发出的确认接收的信号
        props.put("batch.size", "50000");
        props.put("linger.ms", "1000");
        props.put("request.timeout.ms", "60000");
        props.put("acks", "all");
        // 设置大于0的值将使客户端重新发送任何数据，一旦这些数据发送失败
        props.put("retries", Integer.MAX_VALUE);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // props.put("schema.registry.url", "by201:8081");
        // props.put("security.protocol", "SASL_PLAINTEXT");
        // props.put("sasl.mechanism", "PLAIN");
        props.put("max.in.flight.requests.per.connection", 1);
        props.put("block.on.buffer.full", true);
        producer = new KafkaProducer<>(props);
        this.topic = topic;
    }

    @Override
    public void run() {
        int messageNum = 1;
        while (true) {
            String messageStr = new String("测试数据" + messageNum++);
            System.out.println("Send:" + messageStr);
            ProducerRecord<String, String> record =
                new ProducerRecord<>(topic, 2, System.currentTimeMillis(), "test", messageStr);
            try {
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    } else {
                        System.out.println("offset:" + metadata.offset());
                        System.out.println("partition:" + metadata.partition());
                        System.out.println("timestamp:" + metadata.timestamp());
                        System.out.println();
                    }
                });
                if (messageNum == 1000) {
                    producer.flush();
                    producer.close(Duration.ofMillis(5));
                    break;
                }
            } catch (KafkaException e) {
                e.printStackTrace();
            } finally {
                try {
                    sleep(300);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
