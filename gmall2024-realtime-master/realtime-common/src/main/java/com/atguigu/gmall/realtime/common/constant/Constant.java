package com.atguigu.gmall.realtime.common.constant;

/**
 * @author shichunjian
 * @create date 2024-10-24 10:42
 * @Description：
 */
public class Constant {
    public static final String KAFKA_BROKERS = "bigdata.sbx.com:6667";

    public static final String TOPIC_DB = "topic_db";
    public static final String TOPIC_LOG = "topic_log";

    public static final String MYSQL_HOST = "bigdata.sbx.com";
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "PasswordForTest2024_";

    public static final String MYSQL_GMALL_CONF = "gmall_config";

    public static final String MYSQL_G_C_TPD = "table_process_dwd";

    public static final String MYSQL_M_S = "mysql_source";

    public static final String HBASE_HOST = "bigdata.sbx.com";
    public static final String HBASE_NAMESPACE = "gmall";

    public static final String HBASE_DIMTABLE = "dim_base_dic";
    public static final String HBASE_DIMTABLE_TRADEMARK = "dim_base_trademark";
    public static final String HBASE_DIMTABLE_SKU_INFO = "dim_sku_info";
    public static final String HBASE_DIMTABLE_SPU_INFO = "dim_spu_info";
    public static final String HBASE_DIMTABLE_D_B_C3 = "dim_base_category3";
    public static final String HBASE_DIMTABLE_D_B_C2 = "dim_base_category2";
    public static final String HBASE_DIMTABLE_D_B_C1 = "dim_base_category1";
    public static final String HBASE_DIMTABLE_PROVINCE = "dim_base_province";

    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://bigdata.sbx.com:3306?useSSL=false";

    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";

    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";

    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";

    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL = "dwd_trade_order_cancel";

    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "dwd_trade_order_payment_success";

    //public static final String TOPIC_DWD_TRADE_ORDER_PAY_SUC_DETAIL = "dwd_trade_order_pay_suc_detail";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";

    public static final String TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS = "dwd_trade_refund_payment_success";

    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";
    public static final String TOPIC_DWS_T_P_O_W = "dws_trade_province_order_window";

    public static final String DORIS_FE_NODES = "bigdata.sbx.com:8031";

    public static final String DORIS_DATABASE = "gmall_realtime";

    public static final String DORIS_TABLE_KEYWORD_PAGE = "dws_traffic_source_keyword_page_view_window";
    public static final String DORIS_TABLE_T_S_O_W = "dws_trade_sku_order_window";
    public static final String DORIS_TABLE_T_P_O_W = "dws_trade_province_order_window";
    public static final String REDIS_HOST = "bigdata.sbx.com1";
    public static final String REDIS_CLIENT = "redis://bigdata.sbx.com1:6379/0";
    public static final String REDIS_DIMTABLE_SKU_INFO = "dim_sku_info";
    public static final String REDIS_DIMTABLE_SPU_INFO = "dim_spu_info";
    public static final String DWS_TRAFFIC_V_C_A_I_N_P_V_W = "dws_traffic_vc_ch_ar_is_new_page_view_window";
    public static final String DWS_TRAFFIC_H_D_P_V_W = "dws_traffic_home_detail_page_view_window";
    public static final String DWS_U_U_L_W = "dws_user_user_login_window";
    public static final String DWS_U_U_R_W = "dws_user_user_register_window";
    public static final String DWS_T_C_A_U_W = "dws_trade_cart_add_uu_window";
    public static final String DWS_T_S_O_W = "dws_trade_sku_order_window";


    // ClickHouse 驱动
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

    // ClickHouse 连接 URL
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://bigdata.sbx.com1:8123/gmall_realtime";

    public static final String CLICKHOUSE_USER_NAME = "default";
    public static final String CLICKHOUSE_PASSWORD = "123456";

    // ClickHouse 表名（关键词聚合统计表）
    public static final String CLICKHOUSE_TABLE_KEYWORD_PAGE = "dws_traffic_source_keyword_page_view_window";

}
