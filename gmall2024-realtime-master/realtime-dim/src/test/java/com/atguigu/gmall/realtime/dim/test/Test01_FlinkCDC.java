package com.atguigu.gmall.realtime.dim.test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author shichunjian
 * @create date 2024-10-23 16:00
 * @Description：演示FLINKCDC的使用
 */
public class Test01_FlinkCDC {
    public static void main(String[] args) throws Exception {
        //1、基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2、设置并行度
        env.setParallelism(1);
        env.enableCheckpointing(3000);
        //3、使用FlinkCDC读取mysql表中的数据,直接在网页上粘贴过来即可
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("bigdata.sbx.com")
                .port(3306)
                .databaseList("gmall_config") // set captured database
                .tableList("gmall_config.t_user") // set captured table
                .username("root")
                .password("PasswordForTest2024_")
                //这里可以设置要不要同步历史数据，通过该括号里面的函数，默认事要的
//                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();
        //已有，注视掉即可
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint  开启检查点，挪到上面即可
//        env.enableCheckpointing(3000);

        //"op":"r":{"before":null,"after":{"id":1,"name":"zs","age":20},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall_config","sequence":null,"table":"t_user","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1729673160637,"transaction":null}
        //"op":"r":{"before":null,"after":{"id":2,"name":"ls","age":30},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall_config","sequence":null,"table":"t_user","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1729673160638,"transaction":null}
        //"op":"c":{"before":null,"after":{"id":3,"name":"scj","age":33},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1729674570000,"snapshot":"false","db":"gmall_config","sequence":null,"table":"t_user","server_id":1,"gtid":null,"file":"mysql-bin.000018","pos":386,"row":0,"thread":101,"query":null},"op":"c","ts_ms":1729674570920,"transaction":null}
        //"op":"u":{"before":{"id":3,"name":"scj","age":33},"after":{"id":3,"name":"scj_1","age":33},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1729674602000,"snapshot":"false","db":"gmall_config","sequence":null,"table":"t_user","server_id":1,"gtid":null,"file":"mysql-bin.000018","pos":704,"row":0,"thread":101,"query":null},"op":"u","ts_ms":1729674601990,"transaction":null}
        //"op":"d":{"before":{"id":3,"name":"scj_1","age":33},"after":null,"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1729674620000,"snapshot":"false","db":"gmall_config","sequence":null,"table":"t_user","server_id":1,"gtid":null,"file":"mysql-bin.000018","pos":1030,"row":0,"thread":101,"query":null},"op":"d","ts_ms":1729674620875,"transaction":null}
        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // set 4 parallel source tasks 并行度上面设置了，这里注释掉即可，下面打印的也去掉
//                .setParallelism(4)
//                .print().setParallelism(1);
                .print(); // use parallelism 1 for sink to keep message ordering

        env.execute("Print MySQL Snapshot + Binlog");

    }
}
