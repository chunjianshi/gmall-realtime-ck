package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author shichunjian
 * @create date 2024-11-10 12:59
 * @Description：支付成功事实表
 *  需要启动的进程： zk、kafka、hdfs、hbase、maxwell、DwdTradeOrderDetail、DwdTradeOrderPaySucDetail
 */
public class DwdTradeOrderPaySucDetail extends BaseSQLApp {
    public static void main(String[] args) {
        //TODO 准备流表执行环境
        new DwdTradeOrderPaySucDetail().start(10016, 4, Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        //TODO 从下单事务事实表读取数据 创建动态表 这里只是立刻建表，但是不会立刻取数据
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_ORDER_DETAIL + " ( " +
                "id string," +
                "order_id string," +
                "user_id string," +
                "sku_id string," +
                "sku_name string," +
                "province_id string," +
                "activity_id string," +
                "activity_rule_id string," +
                "coupon_id string," +
                "date_id string," +
                "create_time string," +
                "sku_num string," +
                "split_original_amount string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "split_total_amount string," +
                "ts bigint," +
                "et AS TO_TIMESTAMP_LTZ(ts, 0)," +
                "watermark for et as et - interval '3' second " +
                ")" +
                SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));

        //TODO  从topic_db主题中读取数据 创建动态表
        readOdsDb(tableEnv, Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);

        //TODO  过滤出支付成功数据 条件是 kafka的topic的key的database 和 table 及数据中的data 和 old的值进行过滤的 sqlQuery是有数据的及返回值
        Table paymentInfo = tableEnv.sqlQuery("select " +
                "data['user_id'] AS user_id, " +
                "data['order_id'] AS order_id, " +
                "data['payment_type'] AS payment_type, " +
                "data['callback_time'] AS callback_time, " +
                "`pt`, " + // 修改这里，确保 `pt` 使用反引号包围
                "ts, " +
                "et " +
                "from " + Constant.TOPIC_DB +
                " where `table`='payment_info' " +
                " and `type`='update' " +
                " and `old`['payment_status'] is not null " +
                " and data['payment_status']='1602'"); // 修改：确保 data 字段用反引号

        tableEnv.createTemporaryView("payment_info", paymentInfo);

        //TODO  从HBase中读取字典数据
        readBaseDic(tableEnv, Constant.HBASE_DIMTABLE);

        //TODO  过滤后的3个表（下单表，支付成功表和维度表）数据进行关联  --- lookup join 关联的结果和下单数据进行关联 --- IntervalJoin
        Table result = tableEnv.sqlQuery("select " +
                "od.id AS order_detail_id, " +
                "od.order_id, " +
                "od.user_id, " +
                "od.sku_id, " +
                "od.sku_name, " +
                "od.province_id, " +
                "od.activity_id, " +
                "od.activity_rule_id, " +
                "od.coupon_id, " +
                "pi.payment_type AS payment_type_code, " +
                "dic.dic_name AS payment_type_name, " +
                "pi.callback_time, " +
                "od.sku_num, " +
                "od.split_original_amount, " +
                "od.split_activity_amount, " +
                "od.split_coupon_amount, " +
                "od.split_total_amount AS split_payment_amount, " +
                "pi.ts " +
                "from payment_info pi " +
                "join dwd_trade_order_detail od " +
                "on pi.order_id=od.order_id " +
                "and od.et >= pi.et - interval '30' minute " +
                "and od.et <= pi.et + interval '5' second " +
                "join base_dic for system_time as of pi.pt as dic " +
                "on pi.payment_type=dic.dic_code");

        //TODO  将关联的结果写到kafka主题中
        //创建表
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS + " (" +
                "order_detail_id string, " +
                "order_id string, " +
                "user_id string, " +
                "sku_id string, " +
                "sku_name string, " +
                "province_id string, " +
                "activity_id string, " +
                "activity_rule_id string, " +
                "coupon_id string, " +
                "payment_type_code string, " +
                "payment_type_name string, " +
                "callback_time string, " +
                "sku_num string, " +
                "split_original_amount string, " +
                "split_activity_amount string, " +
                "split_coupon_amount string, " +
                "split_payment_amount string, " +
                "ts bigint, " +
                "PRIMARY KEY (order_detail_id) NOT ENFORCED" +
                ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));

        //写入
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
    }
}
