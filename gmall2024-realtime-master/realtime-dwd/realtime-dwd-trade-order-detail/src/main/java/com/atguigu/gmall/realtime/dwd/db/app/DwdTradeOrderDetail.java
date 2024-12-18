package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * FlinkSQL下单明细事实表
 */
public class DwdTradeOrderDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(10014, 4, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        // 读取原始表DOS层的topic_db 创建动态表
        readOdsDb(tableEnv, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);

        // 过滤所需的订单明细表
        Table orderDetail = tableEnv.sqlQuery("SELECT " +
                "`data`['id'] AS id," +
                "`data`['order_id'] AS order_id," +
                "`data`['sku_id'] AS sku_id," +
                "`data`['sku_name'] AS sku_name," +
                "`data`['create_time'] AS create_time," +
                "`data`['source_id'] AS source_id," +
                "`data`['source_type'] AS source_type," +
                "`data`['sku_num'] AS sku_num," +
                "CAST(CAST(`data`['sku_num'] AS DECIMAL(16,2)) * CAST(`data`['order_price'] AS DECIMAL(16,2)) AS STRING) AS split_original_amount," +
                "`data`['split_total_amount'] AS split_total_amount," +
                "`data`['split_activity_amount'] AS split_activity_amount," +
                "`data`['split_coupon_amount'] AS split_coupon_amount," +
                "ts " +
                "FROM topic_db " +
                "WHERE `table` = 'order_detail' " +  // 改动1：用反引号包裹 `table`
                "AND `type` = 'insert'");
        tableEnv.createTemporaryView("order_detail", orderDetail);

        // 过滤所需的订单表
        Table orderInfo = tableEnv.sqlQuery("SELECT " +
                "`data`['id'] AS id," +
                "`data`['user_id'] AS user_id," +
                "`data`['province_id'] AS province_id " +
                "FROM topic_db " +
                "WHERE `table` = 'order_info' " +  // 改动2：用反引号包裹 `table`
                "AND `type` = 'insert'");
        tableEnv.createTemporaryView("order_info", orderInfo);

        // 过滤所需的明细活动关联表
        Table orderDetailActivity = tableEnv.sqlQuery("SELECT " +
                "`data`['order_detail_id'] AS order_detail_id," +
                "`data`['activity_id'] AS activity_id," +
                "`data`['activity_rule_id'] AS activity_rule_id " +
                "FROM topic_db " +
                "WHERE `table` = 'order_detail_activity' " +  // 改动3：用反引号包裹 `table`
                "AND `type` = 'insert'");
        tableEnv.createTemporaryView("order_detail_activity", orderDetailActivity);

        // 过滤所需的明细优惠券关联表
        Table orderDetailCoupon = tableEnv.sqlQuery("SELECT " +
                "`data`['order_detail_id'] AS order_detail_id," +
                "`data`['coupon_id'] AS coupon_id " +
                "FROM topic_db " +
                "WHERE `table` = 'order_detail_coupon' " +  // 改动4：用反引号包裹 `table`
                "AND `type` = 'insert'");
        tableEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);

        // 关联上述的4张表 创建动态表
        Table result = tableEnv.sqlQuery("SELECT " +
                "od.id," +
                "od.order_id," +
                "oi.user_id," +
                "od.sku_id," +
                "od.sku_name," +
                "oi.province_id," +
                "act.activity_id," +
                "act.activity_rule_id," +
                "cou.coupon_id," +
                "date_format(od.create_time, 'yyyy-MM-dd') AS date_id," +
                "od.create_time," +
                "od.sku_num," +
                "od.split_original_amount," +
                "od.split_activity_amount," +
                "od.split_coupon_amount," +
                "od.split_total_amount," +
                "od.ts " +
                "FROM order_detail od " +
                "JOIN order_info oi ON od.order_id = oi.id " +
                "LEFT JOIN order_detail_activity act ON od.id = act.order_detail_id " +
                "LEFT JOIN order_detail_coupon cou ON od.id = cou.order_detail_id");

        // 将创建的关联后的动态表数据写入Kafka主题
        tableEnv.executeSql("CREATE TABLE " + Constant.TOPIC_DWD_TRADE_ORDER_DETAIL + " ( " +
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
                "ts bigint," +
                "PRIMARY KEY(id) NOT ENFORCED " +
                ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));

        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
}
