package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Description: 交易域取消订单事务事实表
 * 需要启动的进程： zk、kafka、maxwell、DwdTradeOrderDetail、DwdTradeOrderCancelDetail
 */
public class DwdTradeOrderCancelDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderCancelDetail().start(10015, 4, Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));

        // 读取 ODS 层的 topic_db 创建动态表
        readOdsDb(tableEnv, Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);

        // 创建 DWD 层订单详情表（依赖于 Kafka 中的 DWD 层下单表）
        tableEnv.executeSql("CREATE TABLE " + Constant.TOPIC_DWD_TRADE_ORDER_DETAIL + " (" +
                "id STRING," +
                "order_id STRING," +
                "user_id STRING," +
                "sku_id STRING," +
                "sku_name STRING," +
                "province_id STRING," +
                "activity_id STRING," +
                "activity_rule_id STRING," +
                "coupon_id STRING," +
                "date_id STRING," +
                "create_time STRING," +
                "sku_num STRING," +
                "split_original_amount STRING," +
                "split_activity_amount STRING," +
                "split_coupon_amount STRING," +
                "split_total_amount STRING," +
                "ts STRING" +
                ") " + SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, Constant.TOPIC_DWD_TRADE_ORDER_CANCEL));

        // 从 topic_db 过滤出订单取消数据并创建动态表 orderCancel
        Table orderCancel = tableEnv.sqlQuery("SELECT " +
                "`data`['id'] AS id, " +
                "`data`['operate_time'] AS operate_time, " +
                "`ts` " +
                "FROM topic_db " +
                "WHERE `database` = 'gmall' " +
                "AND `table` = 'order_info' " +
                "AND `type` = 'update' " +
                "AND `old`['order_status'] = '1001' " +
                "AND `data`['order_status'] = '1003'");
        tableEnv.createTemporaryView("order_cancel", orderCancel);

        // 关联订单详情表和订单取消表，创建结果表
        Table result = tableEnv.sqlQuery("SELECT " +
                "od.id," +
                "od.order_id," +
                "od.user_id," +
                "od.sku_id," +
                "od.sku_name," +
                "od.province_id," +
                "od.activity_id," +
                "od.activity_rule_id," +
                "od.coupon_id," +
                "DATE_FORMAT(oc.operate_time, 'yyyy-MM-dd') AS order_cancel_date_id," +
                "oc.operate_time AS cancel_time," +
                "od.sku_num," +
                "od.split_original_amount," +
                "od.split_activity_amount," +
                "od.split_coupon_amount," +
                "od.split_total_amount," +
                "oc.ts " +
                "FROM " + Constant.TOPIC_DWD_TRADE_ORDER_DETAIL + " od " +
                "JOIN order_cancel oc " +
                "ON od.order_id = oc.id");

        // 创建 Kafka 输出表，用于写入关联后的订单取消数据
        tableEnv.executeSql("CREATE TABLE " + Constant.TOPIC_DWD_TRADE_ORDER_CANCEL + " (" +
                "id STRING," +
                "order_id STRING," +
                "user_id STRING," +
                "sku_id STRING," +
                "sku_name STRING," +
                "province_id STRING," +
                "activity_id STRING," +
                "activity_rule_id STRING," +
                "coupon_id STRING," +
                "date_id STRING," +
                "cancel_time STRING," +
                "sku_num STRING," +
                "split_original_amount STRING," +
                "split_activity_amount STRING," +
                "split_coupon_amount STRING," +
                "split_total_amount STRING," +
                "ts BIGINT," +
                "PRIMARY KEY (id) NOT ENFORCED" +
                ") " + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL));

        // 将结果数据写入 Kafka
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);
    }
}
