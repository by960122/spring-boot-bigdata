package com.example.config;

import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Data;

/**
 * @author: BYDylan
 * @date: 2023/11/05
 * @description: 统一属性控制类,获取配置文件属性
 */
@Data
@ConfigurationProperties(prefix = DataSourceProperties.DS, ignoreUnknownFields = false)
class DataSourceProperties {
    final static String DS = "spring.datasource";
    private Map<String, String> hive;
    private Map<String, String> common;
}