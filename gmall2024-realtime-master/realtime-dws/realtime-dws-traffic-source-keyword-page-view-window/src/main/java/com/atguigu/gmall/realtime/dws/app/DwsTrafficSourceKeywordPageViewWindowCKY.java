package com.atguigu.gmall.realtime.dws.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.bean.KeywordBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import com.atguigu.gmall.realtime.dws.function.KeywordUDTF;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author shichunjian
 * @create date 2024-12-18 上午10:51
 * @Description：
 */
public class DwsTrafficSourceKeywordPageViewWindowCKY extends BaseSQLApp {

    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(
                10021,
                4,
                "dws_traffic_source_keyword_page_view_window"
        );
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
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
        resultTable.execute().print();
        // 创建 ClickHouse 表，并写入数据
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS " + Constant.CLICKHOUSE_TABLE_KEYWORD_PAGE + " (" +
                "  stt STRING, " +
                "  edt STRING, " +
                "  cur_date STRING, " +
                "  keyword STRING, " +
                "  keyword_count BIGINT " +
                ") WITH (" +
                "'connector' = 'jdbc', " +
                "'url' = 'jdbc:clickhouse://bigdata.sbx.com1:8123/gmall_realtime', " +
                "'driver' = 'ru.yandex.clickhouse.ClickHouseDriver', " +
                "'username' = 'default', " +
                "'password' = '123456', " +
                "'table-name' = '" + Constant.CLICKHOUSE_TABLE_KEYWORD_PAGE + "', " +
                "'batch.size' = '1000', " +
                "'batch.interval' = '200' " +
                ")");

        // Insert the result into the ClickHouse table
        resultTable.executeInsert(Constant.CLICKHOUSE_TABLE_KEYWORD_PAGE);
    }
}
