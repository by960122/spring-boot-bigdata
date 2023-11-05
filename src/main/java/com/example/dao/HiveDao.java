package com.example.dao;

import java.util.LinkedHashMap;
import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Repository;

/**
 * @author: BYDylan
 * @date: 2023/11/05
 * @description: 注入hive数据源
 */
@Mapper
@Repository
public interface HiveDao {
    @Select("${sql}")
    List<LinkedHashMap<String, String>> execSql(String sql);
}