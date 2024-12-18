package com.atguigu.gmall.realtime.dim.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class TestHBase {

//    private static final String ZOOKEEPER_QUORUM = "192.168.20.33"; // Zookeeper 地址
    private static final String ZOOKEEPER_QUORUM = "bigdata.sbx.com"; // Zookeeper 地址
    private static final String HBASE_TABLE_NAME = "gmall:test_table"; // 表名
    private static final String COLUMN_FAMILY = "info"; // 列族名称

    public static void main(String[] args) {
        // 创建 HBase 配置
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
        config.set("hbase.zookeeper.property.clientPort", "2181"); // 默认端口
        config.set("zookeeper.znode.parent", "/hbase-unsecure"); // 修改为 /hbase-unsecure

        // 建立连接
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            System.out.println("成功建立 HBase 连接");

            // 创建表
            createHBaseTable(admin, "gmall", "test_table", COLUMN_FAMILY);

            // 写入数据
            writeDataToHBase(connection, HBASE_TABLE_NAME, "row1", COLUMN_FAMILY, "name", "test_name");
            writeDataToHBase(connection, HBASE_TABLE_NAME, "row1", COLUMN_FAMILY, "age", "25");

        } catch (IOException e) {
            System.out.println("出现 IOException: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void createHBaseTable(Admin admin, String namespace, String tableName, String... families) throws IOException {
        if (families.length < 1) {
            System.out.println("至少需要一个列族");
            return;
        }

        TableName tableNameObj = TableName.valueOf(namespace + ":" + tableName);
        System.out.println("检查表是否存在: " + tableNameObj);

//        if (admin.tableExists(tableNameObj)) {
//            System.out.println("表空间 " + namespace + " 下的表 " + tableName + " 已存在");
//            return;
//        }
//
//        System.out.println("表不存在，准备创建新表: " + tableNameObj);
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);
        for (String family : families) {
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            System.out.println("输出 family: " + family);
        }

        admin.createTable(tableDescriptorBuilder.build());
        System.out.println("表空间 " + namespace + " 下的表 " + tableName + " 创建成功");
    }

    private static void writeDataToHBase(Connection connection, String tableName, String rowKey, String columnFamily, String qualifier, String value) throws IOException {
        System.out.println("开始写入数据: rowKey=" + rowKey + ", columnFamily=" + columnFamily + ", qualifier=" + qualifier + ", value=" + value);
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
            System.out.println("数据写入成功: rowKey=" + rowKey + ", columnFamily=" + columnFamily + ", qualifier=" + qualifier + ", value=" + value);
        } catch (IOException e) {
            System.out.println("写入数据失败: " + e.getMessage());
            throw e;
        }
    }
}
