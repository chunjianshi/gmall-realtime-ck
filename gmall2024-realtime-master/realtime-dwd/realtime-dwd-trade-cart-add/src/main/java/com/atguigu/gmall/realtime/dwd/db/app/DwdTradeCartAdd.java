package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.fs.shell.Concat;

/**
 * @author shichunjian
 * @create date 2024-11-07 18:59
 * @Description：加购事实表
 */
public class DwdTradeCartAdd extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeCartAdd().start(
                10013,
                4,
                Constant.TOPIC_DWD_TRADE_CART_ADD);
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        //TODO 1.获取kafka的topic_db主题的数据 创建flink的动态表
        readOdsDb(tableEnv,Constant.TOPIC_DWD_TRADE_CART_ADD);
        //TODO 2.过滤数据，只保留加购事实表的数据 table = 'dwd_trade_cart_add' and type = 'insert' or type = 'update' and data > old
        Table cartInfo = tableEnv.sqlQuery("select \n" +
                "`data`['id'] as id,\n" +
                "`data`['user_id'] as user_id,\n" +
                "`data`['sku_id'] as sku_id,\n" +
                "IF((type = 'update' and (`old`['sku_num'] is not null) and (CAST(`data`['sku_num'] AS INT) > CAST(`old`['sku_num'] AS INT))),CAST((CAST(`data`['sku_num'] AS INT) - CAST(`old`['sku_num'] AS INT)) AS STRING), `data`['sku_num']) as sku_num,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where \n" +
                "`table` = 'cart_info'\n" +
                "and\n" +
                "(type = 'insert' \n" +
                "or\n" +
                "(type = 'update' and (`old`['sku_num'] is not null) and (CAST(`data`['sku_num'] AS INT) > CAST(`old`['sku_num'] AS INT))))");
        //cartInfo.execute().print();
        //TODO 3.对处理后的数据，写入到kafka
        // 3.1 创建kafka的动态表
        tableEnv.executeSql("create table "+Constant.TOPIC_DWD_TRADE_CART_ADD+"(\n" +
                "id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "sku_num string,\n" +
                "ts BIGINT,\n" +
                "PRIMARY KEY (id) NOT ENFORCED \n" +
                ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_CART_ADD));
        // 3.2 数据写入动态表
        cartInfo.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);

    }
}
