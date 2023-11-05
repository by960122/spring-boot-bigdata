package com.example.kafka.consumer;

import java.io.*;
import java.sql.Connection;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * @author: BYDylan
 * @date: 2023/11/05
 * @description: Kafka消费线程
 */
public class KafkaConsumerThread extends Thread {
    private final KafkaConsumer<String, String> consumer;
    private Connection conn = null;

    public KafkaConsumerThread(String topics, String brokerList, String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("group.id", groupId);
        // 自动提交位移
        props.put("enable.auto.commit", "true");
        // consumer向zookeeper提交offset的频率，单位是秒
        props.put("auto.commit.interval.ms", "1000");
        // zookeeper会话超时时间
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // props.put("security.protocol", "SASL_PLAINTEXT");
        // props.put("sasl.mechanism", "PLAIN");
        // Kafka-manager 监控地址
        // props.put("schema.registry.url", "by201:8081");
        this.consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topics));
    }

    @Override
    public void run() {
        File file = new File("C:\\Users\\冰忆\\Desktop\\test_file.txt");
        OutputStream outputStream = null;
        BufferedOutputStream bos = null;
        StringBuilder sb = new StringBuilder();
        // 如果文件大于3KB则退出
        while (file.length() / 1024 < 3) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(Thread.currentThread().getName() + " 分区: " + record.partition() + "   offset: "
                    + record.offset() + "   key:" + record.key() + "  value:" + record.value());
                try {
                    outputStream = new FileOutputStream(file);
                    bos = new BufferedOutputStream(outputStream);
                    sb.append(record.partition()).append("|").append(record.offset()).append("|").append(record.key())
                        .append("|").append(record.value() + "\n");
                    bos.write(sb.toString().getBytes());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                // GenericData.Record r = record.value();
                // for (Schema.Field f : r.getSchema().getFields()) {
                // Object fieldRecord = r.get(f.name());
                // Object filedValue = null;
                // if (fieldRecord instanceof ByteBuffer) {
                // int scale = 0;
                // if (f.schema().getType().getName().equalsIgnoreCase("union")) {
                // List<org.apache.avro.Schema> types = f.schema().getTypes();
                // for (org.apache.avro.Schema schema : types) {
                // JsonNode node = schema.getJsonProp("connect.parameters");
                // if (node != null && node.get("scale") != null) {
                // scale = schema.getJsonProp("connect.parameters").get("scale").asInt();
                // break;
                // }
                // }
                // } else if (f.schema().getType().getName().equalsIgnoreCase("bytes")) {
                // scale = f.schema().getJsonProp("connect.parameters").get("scale").asInt();
                // }
                // ByteBuffer buffer = (ByteBuffer) fieldRecord;
                // filedValue = Decimal.toLogical(Decimal.schema(scale), buffer.array());
                // } else if (fieldRecord != null) {
                // filedValue = fieldRecord.toString();
                // }
                // System.out.print(f.name() + ":" + filedValue + " ");
                // }
                // System.out.println();
            }
            try {
                bos.close();
                outputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println(file.getAbsoluteFile() + " : " + file.length() / 1024 + " KB");
    }
}
