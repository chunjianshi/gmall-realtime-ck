package com.atguigu.gmall.realtime.dws.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.bean.KeywordBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.ClickHouseUtil;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import com.atguigu.gmall.realtime.dws.function.KeywordUDTF;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author shichunjian
 * @create date 2024-11-18 13:45
 * @Description：搜索关键词的聚合统计
 */
public class DwsTrafficSourceKeywordPageViewWindowCK {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1.1 状态后端设置
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(
//                3, Time.days(1), Time.minutes(1)
//        ));
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(
//                "hdfs://bigdata.sbx.com:8020/ck"
//        );
//        System.setProperty("HADOOP_USER_NAME", "flink");

        //TODO 注册自定义函数到表执行环境中
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);
        //TODO 从页面日志事实表中读取数据， 创建动态表 ，并制定 Watermark 的生成策略以及提取事件的时间字段
        tableEnv.executeSql("create table page_log (\n" +
                " common map<string, string>, " +
                " page map<string, string>, " +
                " ts bigint, " +
                " et as to_timestamp_ltz(ts, 3), " +
                " WATERMARK FOR et as et"+
                ")" +
                SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRAFFIC_PAGE,"dws_traffic_source_keyword_page_view_window"));
        //tableEnv.executeSql("select * from page_log").print();
        //TODO 过滤出搜索行为
        Table searchTable = tableEnv.sqlQuery("select " +
                "page['item'] fullword, " +
                "et " +
                "from page_log " +
                "where ( page['last_page_id'] ='search' " +
                "        or page['last_page_id'] ='home' " +
                "       )" +
                "and page['item_type']='keyword' " +
                "and page['item'] is not null ");
        //searchTable.execute().print();
        tableEnv.createTemporaryView("search_table",searchTable);
        //TODO 调用自定义函数完成分词  并和原表的其他字段进行 join
        Table splitTable = tableEnv.sqlQuery("select " +
                " keyword, " +
                " et " +
                "from search_table," +
                "lateral table(ik_analyze(fullword)) t(keyword) ");
        tableEnv.createTemporaryView("split_table",splitTable);
        //tableEnv.executeSql("select * from split_table").print();
        //TODO 分组、开窗、聚合
        Table resultTable = tableEnv.sqlQuery("SELECT \n" +
                " date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt,\n" +
                " date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt,\n" +
                " date_format(window_start, 'yyyy-MM-dd') cur_date,\n" +
                " keyword,\n" +
                " count(*) keyword_count\n" +
                "  FROM TABLE(\n" +
                "    TUMBLE(TABLE split_table, DESCRIPTOR(et), INTERVAL '10' second))\n" +
                "  GROUP BY window_start, window_end,keyword");
        //tableEnv.createTemporaryView("result_table",resultTable);
        //tableEnv.executeSql("select * from result_table").print();
        //resultTable.execute().print();

        //TODO 6.将动态表转换为流
        DataStream<KeywordBean> keywordBeanDataStream = tableEnv.toAppendStream(resultTable, KeywordBean.class);
        keywordBeanDataStream.print(">>>>>>>>>>>>");

        //TODO 配置 JdbcSink，写入 ClickHouse
        keywordBeanDataStream.addSink(JdbcSink.sink(
                "INSERT INTO dws_traffic_source_keyword_page_view_window (stt, edt, cur_date, keyword, keyword_count) VALUES (?, ?, ?, ?, ?)",
                (ps, element) -> {
                    ps.setString(1, element.getStt());
                    ps.setString(2, element.getEdt());
                    ps.setString(3, element.getCur_date());
                    ps.setString(4, element.getKeyword());
                    ps.setInt(5, element.getKeyword_count().intValue());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000) // 每次批量插入 1000 条
                        .withBatchIntervalMs(200) // 每 200ms 触发一次写入
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(""+Constant.CLICKHOUSE_URL+"")
                        .withDriverName(""+Constant.CLICKHOUSE_DRIVER+"")
                        .withUsername(""+Constant.CLICKHOUSE_USER_NAME+"")
                        .withPassword(""+Constant.CLICKHOUSE_PASSWORD+"")
                        .build()
        ));

        //TODO 启动 Flink 作业
        env.execute("DwsTrafficSourceKeywordPageViewWindow");

    }

}
