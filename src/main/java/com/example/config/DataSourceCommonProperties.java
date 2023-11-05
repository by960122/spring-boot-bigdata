package com.example.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Data;

/**
 * @author: BYDylan
 * @date: 2023/11/05
 * @description: 扩展连接池,通用配置属性,可应用到所有数据源
 */
@Data
@ConfigurationProperties(prefix = DataSourceCommonProperties.DS, ignoreUnknownFields = false)
class DataSourceCommonProperties {
    final static String DS = "spring.datasource.common.config";

    private int initialSize = 10;
    private int minIdle;
    private int maxIdle;
    private int maxActive;
    private int maxWait;
    private int timeBetweenEvictionRunsMillis;
    private int minEvictableIdleTimeMillis;
    private String validationQuery;
    private boolean testWhileIdle;
    private boolean testOnBorrow;
    private boolean testOnReturn;
    private boolean poolPreparedStatements;
    private int maxOpenPreparedStatements;
    private String filters;
    private String mapperLocations;
    private String typeAliasPackage;
}
