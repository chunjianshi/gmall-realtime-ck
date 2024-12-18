package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.tasks.mailbox.Mail;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.log4j.chainsaw.Main;

/**
 * @author shichunjian
 * @create date 2024-11-04 16:14
 * @Description：使用FlinkSQL 实现评论事实表
 */
public class DwdInteractionCommentInfo extends BaseSQLApp {
    //环境准备，直接引用封装的方法，然后传参即可
    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(10012,4,Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        //TODO 3.从kafka的topic_db主题中读取数据 创建动态表(这里是从kafka读取到flink中的临时表)     ---kafka连接器
        readOdsDb(tableEnv,Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
        //这里查询的是flink的临时表
        //tableEnv.executeSql("select * from topic_db").print();
        //TODO 4.过滤出评论数据                              ---where table = 'comment_info' && type = 'insert'  一般评论只能增删，不能更改
        Table commentInfo = tableEnv.sqlQuery("select \n" +
                "    `data`['id'] id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    `data`['appraise'] appraise,\n" +
                "    `data`['comment_txt'] comment_txt,\n" +
                "    ts,\n" +
                "    pt\n" +
                " from topic_db where `table` = 'comment_info' and `type` = 'insert' ");
        //commentInfo.execute().print();
        //将表对象注册到表执行环境中
        tableEnv.createTemporaryView("comment_info",commentInfo);
        //TODO 5.从HBase中读取字典维度数据 创建动态表           ---hbase连接器
        readBaseDic(tableEnv,Constant.HBASE_DIMTABLE);
        //TODO 6.将kafka读取的评论表和HBase读取的维度表进行关联  --- lookup Join 需要将4步中，注册一个表对象
        Table joinedTable = tableEnv.sqlQuery("SELECT \n" +
                "  id,\n" +
                "  user_id,\n" +
                "  sku_id,\n" +
                "  appraise,\n" +
                "  dic.dic_name as appraise_name,\n" +
                "  comment_txt,\n" +
                "  ts\n" +
                "FROM comment_info AS c\n" +
                "  JOIN base_dic FOR SYSTEM_TIME AS OF c.pt AS dic\n" +
                "    ON c.appraise = dic.dic_code");
        //joinedTable.execute().print();
        //TODO 7.将关联后的结果表写入kafka新的主题中            --- upsert kafka连接器
        //7.1 创建动态表和要写入的主题表进行映射
        tableEnv.executeSql("CREATE TABLE "+Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO+" (\n" +
                "  id string,\n" +
                "  user_id string,\n" +
                "  sku_id string,\n" +
                "  appraise string,\n" +
                "  appraise_name string,\n" +
                "  comment_txt string,\n" +
                "  ts bigint,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") "+SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
        //7.2 写入
        joinedTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);

        //TODO 8.提交（FlinkSQL略,FlinkAPI需要）

    }


}
