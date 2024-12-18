package com.atguigu.gmall.realtime.dws.test;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;

/**
 * @author shichunjian
 * @create date 2024-12-18 上午9:27
 * @Description：Flink 将数据写入 ClickHouse 示例
 */
public class FlinkClickHouseTest {

    // POJO 类
    public static class KeywordPageView {
        public String stt;
        public String edt;
        public String curDate;
        public String keyword;
        public int keywordCount;

        // 构造方法
        public KeywordPageView(String stt, String edt, String curDate, String keyword, int keywordCount) {
            this.stt = stt;
            this.edt = edt;
            this.curDate = curDate;
            this.keyword = keyword;
            this.keywordCount = keywordCount;
        }

        // 无参构造方法（Flink 序列化需要）
        public KeywordPageView() {}
    }

    public static void main(String[] args) throws Exception {
        // 初始化流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建数据流
        DataStream<KeywordPageView> dataStream = env.fromElements(
                new KeywordPageView("2024-12-18 16:38:40", "2024-12-18 16:38:50", "2024-12-18", "抽", 2)
        );

        // 配置 JdbcSink
        SinkFunction<KeywordPageView> jdbcSink = JdbcSink.sink(
                "INSERT INTO dws_traffic_source_keyword_page_view_window (stt, edt, cur_date, keyword, keyword_count) VALUES (?, ?, ?, ?, ?)",
                (ps, element) -> {
                    ps.setString(1, element.stt);
                    ps.setString(2, element.edt);
                    ps.setString(3, element.curDate);
                    ps.setString(4, element.keyword);
                    ps.setInt(5, element.keywordCount);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000) // 每次批量插入 1000 条
                        .withBatchIntervalMs(200) // 每 200ms 触发一次写入
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://bigdata.sbx.com1:8123/gmall_realtime")
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .withUsername("default")
                        .withPassword("123456")
                        .build()
        );

        // 添加 Sink，将数据写入 ClickHouse
        dataStream.addSink(jdbcSink);

        // 启动 Flink 任务
        env.execute("Flink ClickHouse Test");
    }
}
