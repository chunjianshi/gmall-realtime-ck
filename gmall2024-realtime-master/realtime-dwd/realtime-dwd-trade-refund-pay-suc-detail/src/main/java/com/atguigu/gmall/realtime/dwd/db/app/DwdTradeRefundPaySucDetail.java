package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author shichunjian
 * @create date 2024-11-11 7:59
 * @Description：交易域退款成功事实表
 *  需要启动的进程： zk、kafka、hdfs、hbase、maxwell、DwdTradeRefundPaySucDetail
 */
public class DwdTradeRefundPaySucDetail extends BaseSQLApp {
    //指定流表执行环境
    public static void main(String[] args) {
        new DwdTradeRefundPaySucDetail().start(10018,4, Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        //TODO 配置延迟时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));
        //TODO  读取topic_db 创建动态表
        readOdsDb(tableEnv,Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);
        //TODO  读取字典表
        readBaseDic(tableEnv,Constant.HBASE_DIMTABLE);
        //TODO  过滤退款成功数据
        Table refundPayment = tableEnv.sqlQuery("select " +
                "data['id'] id," +
                "data['order_id'] order_id," +
                "data['sku_id'] sku_id," +
                "data['payment_type'] payment_type," +
                "data['callback_time'] callback_time," +
                "data['total_amount'] total_amount," +
                "pt, " +
                "ts "  +
                "from "  +
                Constant.TOPIC_DB +
                " where `database`='gmall' " +
                "and `table`='refund_payment' " +
                "and `type`='update' " +
                "and `old`['refund_status'] is not null " +
                "and `data`['refund_status']='1602'");
        tableEnv.createTemporaryView("refund_payment",refundPayment);
        //TODO  过滤退单表汇总退单成功数据
        Table orderRefundInfo = tableEnv.sqlQuery("select " +
                "data['order_id'] order_id," +
                "data['sku_id'] sku_id," +
                "data['refund_num'] refund_num " +
                "from "  +
                Constant.TOPIC_DB +
                " where `database`='gmall' " +
                "and `table`='order_refund_info' " +
                "and `type`='update' " +
                "and `old`['refund_status'] is not null " +
                "and `data`['refund_status']='0705'");
        tableEnv.createTemporaryView("order_refund_info",orderRefundInfo);
        //TODO  过滤订单表中退款成功的数据
        Table orderInfo = tableEnv.sqlQuery("select " +
                "data['id'] id," +
                "data['user_id'] user_id," +
                "data['province_id'] province_id " +
                "from "  +
                Constant.TOPIC_DB +
                " where `database`='gmall' " +
                "and `table`='order_info' " +
                "and `type`='update' " +
                "and `old`['order_status'] is not null " +
                "and `data`['order_status']='1006'");
        tableEnv.createTemporaryView("order_info",orderInfo);
        //TODO  4张表join
        Table result = tableEnv.sqlQuery( "select " +
                "rp.id," +
                "oi.user_id," +
                "rp.order_id," +
                "rp.sku_id," +
                "oi.province_id," +
                "rp.payment_type," +
                "dic.info.dic_name payment_type_name," +
                "date_format(rp.callback_time,'yyyy-MM-dd') date_id," +
                "rp.callback_time," +
                "ori.refund_num," +
                "rp.total_amount," +
                "rp.ts " +
                "from refund_payment rp " +
                "join order_refund_info ori " +
                "on rp.order_id=ori.order_id and rp.sku_id=ori.sku_id " +
                "join order_info oi " +
                "on rp.order_id=oi.id " +
                "join base_dic for system_time as of rp.pt as dic " +
                "on rp.payment_type=dic.dic_code ");
        //TODO  结果写入到kafka
        //建表
        tableEnv.executeSql("create table "+
                Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS+
                " ("+
                "id string," +
                "user_id string," +
                "order_id string," +
                "sku_id string," +
                "province_id string," +
                "payment_type_code string," +
                "payment_type_name string," +
                "date_id string," +
                "callback_time string," +
                "refund_num string," +
                "refund_amount string," +
                "ts bigint ," +
                "PRIMARY KEY (id) NOT ENFORCED" +
                " )" +
                SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS));
        //写入
        result.executeInsert(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);

    }
}
