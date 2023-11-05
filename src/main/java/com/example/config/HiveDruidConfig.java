package com.example.config;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import com.alibaba.druid.pool.DruidDataSource;

import lombok.extern.slf4j.Slf4j;

/**
 * @author: BYDylan
 * @date: 2023/11/05
 * @description: 配置hive数据源 EnableConfigurationProperties: 将配置类注入到 bean 容器,使 ConfigurationProperties 注解类生效
 */
@Slf4j
@Configuration
@EnableConfigurationProperties({DataSourceProperties.class, DataSourceCommonProperties.class})
public class HiveDruidConfig {
    private final DataSourceProperties dataSourceProperties;
    private final DataSourceCommonProperties dataSourceCommonProperties;

    @Autowired
    public HiveDruidConfig(DataSourceCommonProperties dataSourceCommonProperties,
        DataSourceProperties dataSourceProperties) {
        this.dataSourceCommonProperties = dataSourceCommonProperties;
        this.dataSourceProperties = dataSourceProperties;
    }

    @Bean("hiveDruidDataSource")
    public DataSource dataSource() {
        DruidDataSource datasource = new DruidDataSource();
        System.out.println(dataSourceProperties);
        // 配置数据源属性
        datasource.setUrl(dataSourceProperties.getHive().get("url"));
        datasource.setUsername(dataSourceProperties.getHive().get("username"));
        datasource.setPassword(dataSourceProperties.getHive().get("password"));
        datasource.setDriverClassName(dataSourceProperties.getHive().get("driver-class-name"));

        // 配置统一属性
        datasource.setInitialSize(dataSourceCommonProperties.getInitialSize());
        datasource.setMinIdle(dataSourceCommonProperties.getMinIdle());
        datasource.setMaxActive(dataSourceCommonProperties.getMaxActive());
        datasource.setMaxWait(dataSourceCommonProperties.getMaxWait());
        datasource.setTimeBetweenEvictionRunsMillis(dataSourceCommonProperties.getTimeBetweenEvictionRunsMillis());
        datasource.setMinEvictableIdleTimeMillis(dataSourceCommonProperties.getMinEvictableIdleTimeMillis());
        datasource.setValidationQuery(dataSourceCommonProperties.getValidationQuery());
        datasource.setTestWhileIdle(dataSourceCommonProperties.isTestWhileIdle());
        datasource.setTestOnBorrow(dataSourceCommonProperties.isTestOnBorrow());
        datasource.setTestOnReturn(dataSourceCommonProperties.isTestOnReturn());
        // 加上会报这个错: getMaxFieldSize error
        // datasource.setPoolPreparedStatements(dataSourceCommonProperties.isPoolPreparedStatements());
        // datasource.setMaxOpenPreparedStatements(dataSourceCommonProperties.getMaxOpenPreparedStatements());
        try {
            datasource.setFilters(dataSourceCommonProperties.getFilters());
        } catch (SQLException e) {
            log.error("druid configuration initialization filter error: {}", e);
        }
        return datasource;
    }

    public JdbcTemplate hiveDruidTemplate(@Qualifier("hiveDruidDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}