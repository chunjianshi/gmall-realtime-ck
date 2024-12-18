package com.atguigu.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.dim.function.HBaseSinkFunction;
import com.atguigu.gmall.realtime.dim.function.TableProcessFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import lombok.val;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;


/**
 * @author shichunjian
 * @create date 2024-10-23 17:29
 * @Description：DIM维度层的处理
 * 需要启动的进程
 * zk、kafka、maxwell、hdfs、hbase、DimApp
 * 开发流程
 *  基本环境准备
 *  检查点相关配置
 *  从kafka主题中读取数据
 *  对流中的数据进行类型转换并ETL   json -> jsonObj
 *  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *  使用FlinkCDC读取配置表中的配置信息   维度表广播流
 *  对读取到的配置流数据进行类型转换  jsonStr->实体类对象
 *  根据当前mysql的gmall_config数据库的 table_process_dim 的配置信息到HBase中执行建表或者删除操作（操作mysql的表来实现hbase的建表、删表，修改表字段、添加表字段）
 *      op=d    删表
 *      op=c、r 建表
 *      op=u    先删表，再建表
 *      表建好后，就可以处理数据了！
 *   对配置流数据进行广播---broadcast  配置流是需要用的，如果表维度信息变化，得及时告知
 *   关联主流业务数据以及广播流配置数据---connect  开始处理，维度的数据，这里的数据也是来源于 gmall的数据库里面的维度表（除了维度表还有业务表，得先识别），
 *   识别到维度表后，得先按照hbase需要的表的字段形式进行过滤，保留需要的，去掉冗余的，并且维度表的数据的增删改查的操作行为得获取到 type
 *   对关联后的数据进行处理--process
 *      new TableProcessFunction extend BroadcastProcessFunction{
 *          open:将配置信息预加载到程序中，避免主流数据先到，广播流数据后到，丢失数据的情况
 *          processElement:对主流数据进行处理
 *              获取操作表的表名
 *              根据表名到广播状态中以及configMap中获取对应的配置信息，如果配置信息不为空，说明是维度表，将维度数据发送到下游
 *                  Tuple2<dataJsonObj,配置对象>
 *              在向下游发送维度数据前，先过滤掉不需要传递的数据data
 *              在想下游发送过滤后的维度数据前，添加补充操作类型type
 *          processBroadcastElement:对广播流数据进行处理
 *              op = d 将配置信息从广播状态以为congfigMap中删除掉
 *              op != d 将配置信息放到广播状态以为congfigMap中
 *      }
 *    将流中数据同步到HBase中
 *      class HBaseSinkFunction extends RichSinkFunction{
 *          invock:
 *              type="delete" : 从HBase表中删除数据
 *              type!="delete" : 从HBase表中put数据
 *      }
 *    优化：抽取FlinkSourceUtil工具类
 *         抽取TbaleProcessFunction以及HBaseSinkFunction函数处理
 *         抽取方法
 *         抽取基类---模板方法设计模式
 *    执行流程（以修改了品牌维度表中一条数据为例）
 *        当程序启动的时候，会将mysql的gmall_config库中的table_process_dim配置表中的配置信息加载到configMap以及广播流中（表维度操作，建表、删表、更新表）
 *        mysql修改品牌维度gmall的维度表的数据
 *        binlog会将修改操作记录下来  数据的增删改查
 *        maxwell会从binlog中获取修改的信息，并封装为json格式字符串发送到kafka的topic_db主题中
 *        DimApp应用程序会从topic_db主题中读取数据并进行处理 （数据ETL，过滤及添加操作字段）
 *        根据当前处理的数据的表名判断是否为维度
 *        如果是维度的话，将维度数据传递到下游
 *        将维度数据同步到HBase中
 */
public class DimApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DimApp().start(10001,4,"dim_app",Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 对业务流中数据类型进行转换并进行简单的ETL jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = etl(kafkaStrDS);

        //TODO 使用FlinkCDC读取配置表中的配置信息,对配置流中的数据类型进行转换 json->实体类对象
        SingleOutputStreamOperator<TableProcessDim> tpDS = readTableProcess(env);

        //TODO 根据配置表中的配置信息到HBase中执行建表或者删除表操作
        tpDS = createHBaseTable(tpDS);

        //tpDS.print();
        //TODO 过滤维度数据
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connect(jsonObjDS, tpDS);

        //TODO 将维度数据同步到HBase表中
        dimDS.print();
        writeToHBase(dimDS);

    }

    private static void writeToHBase(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS) {
        dimDS.addSink(new HBaseSinkFunction());
    }

    private static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connect(SingleOutputStreamOperator<JSONObject> jsonObjDS, SingleOutputStreamOperator<TableProcessDim> tpDS) {
        //将配置流中的配置信息进行广播 ---broadcast
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor
                = new MapStateDescriptor<String,TableProcessDim>("mapStateDescriptor", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        //将主流业务数据和广播流配置信息进行关联---connect
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);

        //处理关联后的数据（判断是否为维度）
        //10.1 processElement：处理主流业务
        //10.2 processBroadcastElement:处理广播流配置信息
        SingleOutputStreamOperator<Tuple2<JSONObject,TableProcessDim>> dimDS = connectDS.process(
                new TableProcessFunction(mapStateDescriptor)
        );
        return dimDS;
    }

    private static SingleOutputStreamOperator<TableProcessDim> createHBaseTable(SingleOutputStreamOperator<TableProcessDim> tpDS) {
        tpDS = tpDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {

                    private Connection hbaseConn;

                    @Override
                    public void  open(Configuration parameters) throws Exception{
                        hbaseConn = HBaseUtil.getHBaseConnection();
                        System.out.println("hbase连接成功！"+ hbaseConn);
                    }

                    @Override
                    public void  close() throws Exception{
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                        System.out.println("hbase关闭成功！"+ hbaseConn);
                    }
                    @Override
                    public TableProcessDim map(TableProcessDim tp) throws Exception {
                        //获取对配置表进行的操作的类型
                        String op = tp.getOp();
                        //获取hbase中的维度表的表名
                        String sinkTable = tp.getSinkTable();
                        //获取在hbase表中建表的列族
                        String[] sinkFamilies = tp.getSinkFamily().split(",");
                        if ("d".equals(op)){
                            //从配置表中删除了一条数据 将hbase中对应的表删除掉
                            System.out.println("删除hbase的表集合{}"+ sinkTable);
                            HBaseUtil.dropHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                        }else if ("r".equals(op) || "c".equals(op)){
                            //从配置表中读取了一条数据或者想配置表中添加了一条配置 在hbase中执行建表，如果表已经存在就会略过，所以表存在时也可以执行这个函数
                            //System.out.println("创建hbase的表"+ sinkTable);
                            HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                        }else {
                            //对配置表中的信息进行了修改 先从hbase中将对应的表删除掉，再创建新表
                            System.out.println("更新hbase的表"+ sinkTable);
                            HBaseUtil.dropHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                            HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                        }
                        return tp;
                    }
                }
        ).setParallelism(1);
        return tpDS;
    }

    private static SingleOutputStreamOperator<TableProcessDim> readTableProcess(StreamExecutionEnvironment env) {
        //5.1 创建MysqlSource对象
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("gmall_config", "table_process_dim");
        //5.2 读取数据 封装为流
        DataStreamSource<String> mysqlStrDS = env
                .fromSource(mySqlSource,WatermarkStrategy.noWatermarks(),"mysql_source")
                .setParallelism(1);
        //mysqlStrDS.print();
        //TODO 6.对配置流中的数据类型进行转换 json->实体类对象
        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String jsonStr) throws Exception {
                        //为了处理方便，现将jsonStr转为jsonObj
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String op = jsonObj.getString("op");
                        TableProcessDim tableProcessDim = null;
                        if ("d".equals(op)){
                            //对配置表进行一次删除操作 从before属性中获取删除前的配置信息
                            tableProcessDim = jsonObj.getObject("before",TableProcessDim.class);
                        }else{
                            //对配置表进行读取、添加、修改操作 对after属性中获取最新的配置信息
                            tableProcessDim = jsonObj.getObject("after",TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);
//        tpDS.print();
        return tpDS;
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {

                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        val db = jsonObj.getString("database");
                        String type = jsonObj.getString("type");
                        String data = jsonObj.getString("data");

                        if ("gmall".equals(db)
                                && ("insert".equals(type)
                                || "update".equals(type)
                                || "delete".equals(type)
                                || "bootstrap-insert".equals(type))
                                && data != null
                                && data.length() > 2) {

                            out.collect(jsonObj);
                        }
                    }
                }
        );
        return jsonObjDS;
    }
}
