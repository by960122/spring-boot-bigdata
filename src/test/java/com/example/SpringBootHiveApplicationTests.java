package com.example;

import java.util.LinkedHashMap;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.example.dao.HiveDao;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = SpringBootBigDataApplication.class)
class SpringBootHiveApplicationTests {
    @Autowired
    private HiveDao hiveDao;

    @Test
    void testHive() {
        List<LinkedHashMap<String, String>> resultList = hiveDao.execSql("show databases");
        log.info("返回结果: {}", resultList);
    }

}
