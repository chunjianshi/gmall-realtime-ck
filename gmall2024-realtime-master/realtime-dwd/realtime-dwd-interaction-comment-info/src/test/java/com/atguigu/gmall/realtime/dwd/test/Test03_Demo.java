package com.atguigu.gmall.realtime.dwd.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import redis.clients.jedis.params.SetParams;

/**
 * @author shichunjian
 * @create date 2024-11-03 11:45
 * @Description：通过当前Demo类模拟评论事实表实现过程
 */
public class Test03_Demo {
    public static void main(String[] args) {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO 2.检查点相关配置（略）
        //TODO 3.从kafka的first主题中读取员工数据 并创建动态表
        tableEnv.executeSql(
                "CREATE TABLE emp (\n" +
                        "  empno string,\n" +
                        "  ename string,\n" +
                        "  deptno string,\n" +
                        "  proc_time as PROCTIME()\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'first',\n" +
                        "  'properties.bootstrap.servers' = 'bigdata.sbx.com:6667',\n" +
                        "  'properties.group.id' = 'testGroup',\n" +
                        "  'scan.startup.mode' = 'latest-offset',\n" +
                        "  'format' = 'json'\n" +
                        ")"
        );
        //tableEnv.executeSql("select * from emp").print();
        //TODO 4.从HBase表中读取部门数据 并创建动态表
        tableEnv.executeSql(
                "CREATE TABLE dept (\n" +
                        " deptno string,\n" +
                        " info ROW<dname string>,\n" +
                        " PRIMARY KEY (deptno) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        " 'connector' = 'hbase-2.2',\n" +
                        " 'table-name' = 't_dept',\n" +
                        " 'zookeeper.quorum' = 'bigdata.sbx.com:2181',\n" +
                        " 'zookeeper.znode.parent' = '/hbase-unsecure',\n" +
                        " 'lookup.async' = 'true',\n" +
                        " 'lookup.cache' = 'PARTIAL',\n" +
                        " 'lookup.partial-cache.max-rows' = '500',\n" +
                        " 'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                        " 'lookup.partial-cache.expire-after-access' = '1 hour'\n" +
                        ")"
        );
        //tableEnv.executeSql("select * from dept").print();
        //TODO 5.关联员工和部门数据
        //如果使用lookupjoin，它的底层实现原理和普通的内外连接是完全不同，没有为参与连接的两张表维护状态
        //它是左表进行驱动的（事件驱动），当左表数据到来的时候，发送请求和右表进行关联
        Table joinedTable = tableEnv.sqlQuery("SELECT e.empno,e.ename,d.deptno,d.dname\n" +
                "FROM emp AS e\n" +
                " JOIN dept FOR SYSTEM_TIME AS OF e.proc_time AS d\n" +
                "ON e.deptno = d.deptno");
        //joinedTable.execute().print();
        //TODO 6.将关联的结果数据写入到kafka主题
        //6.1 创建动态表和要写入的主题进行映射
        tableEnv.executeSql("CREATE TABLE emp_dept (\n" +
                "  empno string,\n" +
                "  ename string,\n" +
                "  deptno string,\n" +
                "  dname string,\n" +
                "  PRIMARY KEY (empno) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'second',\n" +
                "  'properties.bootstrap.servers' = 'bigdata.sbx.com:6667',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")");
        //6.2 写入kafka
        //tableEnv.executeSql("insert into dept from joinedTable");
        joinedTable.executeInsert("emp_dept");

    }
}
