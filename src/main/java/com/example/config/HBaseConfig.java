package com.example.config;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * @author: BYDylan
 * @date: 2023/11/5
 * @description:
 */
@Data
@Slf4j
@Configuration
public class HBaseConfig {
    @Value("${hbase.zookeeper.quorum}")
    private String quorum;
    @Value("${hbase.zookeeper.port}")
    private String port;
    @Value("${hadoop.username}")
    private String userName;

    @Bean("hbaseConnect")
    public Connection hbaseConnect() {
        Connection connection = null;
        System.setProperty("HADOOP_USER_HOME", userName);
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", quorum);
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            log.error("获取 hbase 连接失败: {}", e.getMessage());
        }
        return connection;
    }
}
