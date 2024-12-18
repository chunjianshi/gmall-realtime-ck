package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.bean.TransientSink;
import com.atguigu.gmall.realtime.common.constant.Constant;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author shichunjian
 * @create date 2024-12-17 下午3:42
 * @Description：
 */

public class ClickHouseUtil {
    public static <T> SinkFunction<T> getJdbcSink(String sql) {
        return JdbcSink.<T>sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T obj) throws SQLException {
                        Field[] declaredFields = obj.getClass().getDeclaredFields();
                        int skipNum = 0;
                        // 打印日志，查看正在插入的数据
                        System.out.println("准备插入数据: " + obj);
                        for (int i = 0; i < declaredFields.length; i++) {
                            Field declaredField = declaredFields[i];
                            TransientSink transientSink = declaredField.getAnnotation(TransientSink.class);
                            if (transientSink != null) {
                                skipNum++;
                                continue;
                            }
                            declaredField.setAccessible(true);
                            try {
                                Object value = declaredField.get(obj);
                                // 打印字段信息
                                System.out.println("设置字段: " + declaredField.getName() + " 值: " + value);

                                preparedStatement.setObject(i + 1 - skipNum, value);
                            } catch (IllegalAccessException e) {
                                System.out.println("ClickHouse 数据插入 SQL 占位符传参异常 ~");
                                e.printStackTrace();
                            }
                        }
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(5000L)
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(Constant.CLICKHOUSE_DRIVER)
                        .withUrl(Constant.CLICKHOUSE_URL)
                        .build()
        );
    }
}

