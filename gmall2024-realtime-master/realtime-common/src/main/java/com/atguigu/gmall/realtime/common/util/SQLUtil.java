package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;
import javafx.scene.control.TablePosition;

/**
 * @author shichunjian
 * @create date 2024-11-07 6:56
 * @Description：FlinkSQL操作的工具类
 */
public class SQLUtil {
    public static String getKafkaDDL(String topic,String groupId){
        //获取kafka的连接属性
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                        "  'topic' = '"+topic+"',\n" +
                        "  'properties.bootstrap.servers' = 'bigdata.sbx.com:6667',\n" +
                        "  'properties.group.id' = '"+groupId+"',\n" +
                        "  'scan.startup.mode' = 'latest-offset',\n" +
                        "  'format' = 'json'\n" +
                        ")";
    }
    //获取HBase连接器的连接属性
    public  static String getHBaseDDL(String table){
        return "WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = '" + Constant.HBASE_NAMESPACE +":"+table+"',\n" +
                " 'zookeeper.quorum' = 'bigdata.sbx.com:2181',\n" +
                " 'zookeeper.znode.parent' = '/hbase-unsecure',\n" +
                " 'lookup.async' = 'true',\n" +
                " 'lookup.cache' = 'PARTIAL',\n" +
                " 'lookup.partial-cache.max-rows' = '500',\n" +
                " 'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                " 'lookup.partial-cache.expire-after-access' = '1 hour' \n" +
                ")";
    }
    //获取Upsert_kafka连接器的连接属性
    public static String getUpsertKafkaDDL(String topic){
        return "WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '"+ topic +"',\n" +
                "  'properties.bootstrap.servers' = '"+Constant.KAFKA_BROKERS+"',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }

}
