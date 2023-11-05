package com.example.hive;

import java.sql.*;

/**
 * @author: BYDylan
 * @date: 2023/11/05
 * @description:
 */
public class HiveDemo {
    public static void main(String[] args) throws SQLException {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        // 此处用户名需要是有权限操作HDFS的Linux用户
        Connection con = DriverManager.getConnection("jdbc:hive2://192.168.1.201:10000", "root", "");
        Statement stmt = con.createStatement();
        ResultSet res = stmt.executeQuery("select * from hive.test_hive");
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }
    }
}
