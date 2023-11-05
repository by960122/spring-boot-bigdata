package com.example.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

/**
 * @author: BYDylan
 * @date: 2023/11/5
 * @description:
 */
@Slf4j
@Service
public class HBaseService {
    private Connection connection;
    private Admin admin;

    public HBaseService(@Qualifier("hbaseConnect") Connection connection) {
        this.connection = connection;
        try {
            admin = connection.getAdmin();
        } catch (IOException e) {
            log.error("Failec to get admin: {}", e);
        }
    }

    public void createNameSpace(String spaceName) {
        try {
            admin.createNamespace(NamespaceDescriptor.create(spaceName).build());
            // 删除名称空间
            // admin.deleteNamespace(spaceName);
        } catch (IOException e) {
            log.error("Failed to create name space : {} ,", spaceName, e);
            e.printStackTrace();
        }
    }

    public void createTable(String tableName, List<String> familyList) {
        TableName table = TableName.valueOf(tableName);
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(table);
        for (String family : familyList) {
            tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(family));
        }
        try {
            NamespaceDescriptor.create("test").build();
            admin.createTable(tableDescriptorBuilder.build());
            // 删除表
            // admin.deleteTable(table);
            // 获取表描述
            // admin.getDescriptor(table);
            // 批量获取表描述
            // admin.listTableDescriptors();
        } catch (IOException e) {
            log.error("Failed to create table : {} ,", tableName, e);
            e.printStackTrace();
        }
    }

    public void putData(String tableName, String rowkey, String family, Map<String, String> data) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowkey));
            for (String column : data.keySet()) {
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(data.get(column)));
            }
            table.put(put);
        } catch (IOException e) {
            log.error("Failed put Data to hbase : {} , {}", tableName, e);
            e.printStackTrace();
        }
    }

    public void deleteDataColumn(String tableName, String rowKey, String family, String column) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
            table.delete(delete);
        } catch (IOException e) {
            log.error("Failed delete data: {} , {}", tableName, e);
            e.printStackTrace();
        }
    }

    public void deleteDataFamily(String tableName, String rowKey, String family) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addFamily(Bytes.toBytes(family));
            table.delete(delete);
        } catch (IOException e) {
            log.error("Failed delete data: {} , {}", tableName, e);
            e.printStackTrace();
        }
    }

    public void deleteDataFamily(String tableName, String rowKey) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
        } catch (IOException e) {
            log.error("Failed delete data: {} , {}", tableName, e);
            e.printStackTrace();
        }
    }

    public String getColumnValue(String tablename, String rowkey, String family, String column) {
        String returnResult = "";
        try {
            Table table = connection.getTable(TableName.valueOf(tablename));
            Get get = new Get(Bytes.toBytes(rowkey));
            Result result = table.get(get);
            returnResult = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(column)));
        } catch (IOException e) {
            log.error("Failed to get data: {} , {}", tablename, e);
            e.printStackTrace();
        }
        return returnResult;
    }

    public List<String> getColumnValueFilter(String tableName, String family, String targetColumn, String queryColumn,
        String queryColumnValue) {
        List<String> returnList = new ArrayList<>();
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(family),
                Bytes.toBytes(queryColumn), CompareOperator.EQUAL, Bytes.toBytes(queryColumnValue));
            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                returnList.add(Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(targetColumn))));
            }
        } catch (IOException e) {
            log.error("Failed to getColumnValueFilter: {} , {}", tableName, e);
            e.printStackTrace();
        }
        return returnList;
    }

    public List<String> getColumnPrefixFilter(String tableName, String family, String targetColumn,
        String queryColumnPrefix) {
        List<String> returnList = new ArrayList<>();
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            // 多个列名前缀
            // byte[][] queryColumnPrefixArray = {Bytes.toBytes(queryColumnPrefix), Bytes.toBytes(queryColumnPrefix)};
            // MultipleColumnPrefixFilter filter = new MultipleColumnPrefixFilter(queryColumnPrefixArray);

            ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes(queryColumnPrefix));
            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                returnList.add(Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(targetColumn))));
            }
        } catch (IOException e) {
            log.error("Failed to getColumnPrefixFilter: {} , {}", tableName, e);
            e.printStackTrace();
        }
        return returnList;
    }

    public List<String> getRowkeyPrefixFilter(String tableName, String rowkey, String family, String column) {
        List<String> returnList = new ArrayList<>();
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            RowFilter filter = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator(rowkey));
            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                returnList.add(Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(column))));
            }
        } catch (IOException e) {
            log.error("Failed to getRowkeyPrefixFilter: {} , {}", tableName, e);
            e.printStackTrace();
        }
        return returnList;
    }

    public List<String> getScanValue(String tablename, String family, String column) {
        List<String> returnList = new ArrayList<>();
        try {
            Table table = connection.getTable(TableName.valueOf(tablename));
            Scan scan = new Scan();
            ResultScanner resultScanner = table.getScanner(scan);
            // 通过循环输出
            for (Result result : resultScanner) {
                returnList.add(Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(column))));
            }
        } catch (IOException e) {
            log.error("Failed to scan: {} , {}", tablename, e);
            e.printStackTrace();
        }
        return returnList;
    }
}
