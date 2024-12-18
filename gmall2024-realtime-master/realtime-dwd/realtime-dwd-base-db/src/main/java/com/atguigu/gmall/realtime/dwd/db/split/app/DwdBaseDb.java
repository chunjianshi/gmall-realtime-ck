package com.atguigu.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TableProcessDwd;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.dwd.db.split.app.function.BaseDbTableProcessFunction;
import com.mysql.cj.exceptions.ClosedOnExpiredPasswordException;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import javax.swing.text.TabableView;

/**
 * @author shichunjian
 * @create date 2024-11-12 10:47
 * @Description：处理逻辑比较简单的事实表动态分流处理
 * 需要启动的程序：zk、kafka、maxwell、DwdBaseDb
 */
public class DwdBaseDb extends BaseApp {
    //TODO 初始化环境配置，并读取topic_db的数据
    public static void main(String[] args) throws Exception {
        new DwdBaseDb().start(10019,4,"dwd_base_db", Constant.TOPIC_DB);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 对topic_db流中的数据进行类型转换并进行简单的ETL（过滤掉历史数据，只保留最新的实时数据）  主流 jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            String type = jsonObj.getString("type");
                            if (!type.startsWith("bootstrap-")) {
                                out.collect(jsonObj);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException("非标准json");
                        }

                    }
                }
        );
        //jsonObjDS.print();
        //TODO 使用FlinkCDC读取配置表中的配置信息 广播流（配置信息，元数据）
        //创建MySqlSource对象
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource(Constant.MYSQL_GMALL_CONF, Constant.MYSQL_G_C_TPD);
        //读取数据 封装为流
        DataStreamSource<String> mysqlStrDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), Constant.MYSQL_M_S);
        //对流中的数据进行类型转换 jsonStr -> 实体类对象
        SingleOutputStreamOperator<TableProcessDwd> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDwd>() {
                    @Override
                    public TableProcessDwd map(String jsonStr) throws Exception {
                        //"op":"r"
                        //"op":"c"
                        //"op":"u"
                        //"op":"d"

                        //为了处理方便，先将jsonStr转换为jsonObj
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        //获取操作类型
                        String op = jsonObj.getString("op");
                        TableProcessDwd tp = null;
                        if ("d".equals(op)) {
                            //对配置表进行了删除操作 需要从before属性中获取删除前配置信息
                            tp = jsonObj.getObject("before",TableProcessDwd.class);
                        } else {
                            //对配置表进行了读取、更新、插入操作 需要从after属性中获取读取、更新、插入操作前配置信息
                            tp = jsonObj.getObject("after",TableProcessDwd.class);
                        }
                        tp.setOp(op);
                        return tp;
                    }
                }
        );
        //tpDS.print();
        //TODO 对配置流进行广播   ---broadcast
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor
                = new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastDS = tpDS.broadcast(mapStateDescriptor);
        //TODO 关联主流业务数据和广播流中的配置数据  ---connect
        BroadcastConnectedStream<JSONObject, TableProcessDwd> connectDS = jsonObjDS.connect(broadcastDS);
        //TODO 对关联后的数据进行处理   ---process
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> splitDS = connectDS.process(new BaseDbTableProcessFunction(mapStateDescriptor));
        //TODO 将处理逻辑比较简单的事实表数据写入不同的kafka的不同主题中
        //({"id":9884,"ts":1731413360},TableProcessDwd(sourceTable=favor_info, sourceType=insert, sinkTable=dwd_interaction_favor_add, sinkColumns=id,user_id,sku_id,create_time, op=r))
        //splitDS.print();
        splitDS.sinkTo(FlinkSinkUtil.getKafkaSink());
    }
}
