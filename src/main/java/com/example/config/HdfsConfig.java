package com.example.config;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.extern.slf4j.Slf4j;

/**
 * @author: BYDylan
 * @date: 2023/11/05
 * @description:
 */
@Slf4j
@Configuration
public class HdfsConfig {

    @Value("${hdfs.path}")
    private String hdfsPath;
    @Value("${hadoop.username}")
    private String userName;

    @Bean
    public FileSystem fileSystem() {
        FileSystem fs = null;
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        configuration.set("fs.defaultFS", hdfsPath);
        try {
            fs = FileSystem.get(new URI(hdfsPath), configuration, userName);
        } catch (IOException | InterruptedException | URISyntaxException e) {
            log.error("connect hdfs error: ", e);
        }
        return fs;
    }
}
