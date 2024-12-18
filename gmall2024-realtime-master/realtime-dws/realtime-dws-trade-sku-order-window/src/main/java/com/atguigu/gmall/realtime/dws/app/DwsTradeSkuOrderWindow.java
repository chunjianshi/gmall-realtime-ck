package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.gmall.realtime.common.function.DimAsyncFunction;
import com.atguigu.gmall.realtime.common.function.DimMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * @author shichunjian
 * @create date 2024-11-24 16:50
 * @Description：sku粒度下单业务过程聚合统计
 * 需要启动的进程：
 *             zk kafka maxwell hdfs hbase redis doris DwdTradeOrderDetail DwsTradeSkuOrderWindow
 * 开发流程：
 *         基本环境准备、检查点相关设置、从kafka的下单事实表中读取数据、空消息的处理并将流中的数据类型进行转换  jsonStr -> jsonObj
 *         去重：
 *              为什么会产生重复数据？
 *                  我们是从下单事实表中读取数据的，下单事实表由订单表、订单明细表、订单明细活动表、订单明细优惠券表四张表组成
 *                      订单明细表是主表，和订单明细表进行关联的时候，使用的是内连接
 *                      和订单活动表以及订单明细优惠券表进行关联的时候，使用的是左外连接
 *                      如果左外连接，左表数据先到，右表数据后到，查询结果会有3条数据
 *                      左表   null   标记+I
 *                      左表   null   标记-D
 *                      左表   右表    标记+I
 *                      这样的数据发送到kafka主题中，kafka主题会接收到3条消息数据
 *                          左表   null
 *                          null
 *                          左表   右表
 *                      所以我们在从下单事实表中读取数据的时候，需要过滤空消息，并去重
 *         去重前：需要按照唯一键进行分组
 *              去重方案1：  状态 + 定时器
 *                  当第一条数据到来的时候，将数据放到状态中保存起来，并注册5S后执行的定时器
 *                  当第二条数据到来的时候，会用第二条数据的聚合时间和第一条数据的聚合时间进行比较，将时间大的数据放到状态中
 *                  当定时器被触发执行的时候，将状态中的数据发送到下游
 *                  优点： 如果数据出现重复了，只会向下游发送一条数据，数据不会膨胀
 *                  缺点： 时效性差
 *              去重方案2：  状态 + 抵消
 *                  当第一条数据到来的时候，将数据放到状态总，并向下游传递
 *                  当第二条数据到来的时候，将状态中影响到度量值的字段进行取反，传递到下游
 *                  并将第二条数据也向下游传递
 *                  优点：时效性好
 *                  缺点：如果出现重复了，向下游传递3条数据，数据会出现膨胀
 *         指定Watermark以及提取事件时间字段
 *         再次对流中数据进行类型转换   jsonObj -> 实体类对象 （相当于wordcount封装二元组的过程）
 *         按照统计的维度进行分组
 *         开窗
 *         聚合计算
 *         维度关联：
 *                最基本的实现方式：   HBaseUtil -> getRow
 *                优化1： 旁路缓存（Redis）
 *                     思路：
 *                          先从缓存中获取维度数据，如果获取到了维度数据就直接返回，如果没有获取到维度数据，就发送HBase请求到HBase中
 *                          去查找，如果获取到了就直接返回维度信息，并将该查询结果写入到redis缓存中，方便下次查找。并将查找到的维度信息和主
 *                          流进行关联，并将关联后的结果传递到下游进行接力关联！
 *                     选型：
 *                          状态      性能很好，维护性差，数据只能自用
 *                          redis    性能不错，维护性好，数据可通用        √
 *                     关于redis的设置：
 *                                  key     维度表名:主键值
 *                                  type    String
 *                                  expire:  1day  有效时间为1天，避免冷数据常驻内存，给内存带来压力
 *                                  注意：如果维度数据发生了裱花，需要将变化数据清除缓存（删除掉）  DimSinkFunction -> invoke
 *                优化2： 异步IO + 旁路缓存（Redis） + 模板
 *                      为什么使用异步？
 *                          在Flink程序中，想要提升某个算子的处理能力，可以提升这个算子的并行度，但是更多的并行度就需要更多的硬件资源，不可能无限制的
 *                          提升资源，在资源有限的情况下，可以考虑使用异步方式，减少和程序等待时间（差分复用，等待时间去处理别的请求，这样一来，等待时
 *                          间就利用起来了，基本上就没有啥等待时间了，速度就快了！）
 *                      异步使用场景：
 *                          用外部系统的数据，扩展六中数据的时候（主要是请求外部的有太多的延时浪费掉了，所以异步就能利用起来了！默认情况下，如果使用map
 *                          算子，对流中的数据进行处理，底层使用的是同步流的处理方式（单线程，顺序）处理完一个元素后在处理下一个元素，性能较低，所以在
 *                          做维度关联的时候，可以使用Flink提供的发送异步请求的API，进行异步处理）
 *                      异步处理的函数逻辑：
 *                          AsyncDataStream.[un]orderWait(
 *                              流（上一个输出流，这里需要接力补充），
 *                              New  DimAsyncFunction, //发送异步请求，需要实现AsyncFunction接口，这里自定义了一个函数 DimAsyncFunction
 *                              //然后通过抽象模板可以自己简单实现
 *                              超时时间，
 *                              时间单位
 *                          )
 *                      HBaseUtil添加异步读取数据的方法，自定义的
 *                      RedisUtil添加异步读取数据的方法，自定义的
 *                      封装了模板类（还进一步封装了接口），专门发送异步请求进行维度关联
 *                          class DimAsyncFunction extends RichAsyncFunction[asyncInvoke] implements DimJoinFunction[getRowKey（主键）、getTableName（表名）、addDims（关联维度）](
 *                              asyncInvoke:
 *                                  //创建异步编排对象，有返回值  有输入和输出，主要从对象总获取主键
 *                                  CompletableFuture.supplyAsync
 *                                  //执行线程任务  有入参、有返回值(接力传送，上一个的输出流作为下一个输入流的开始)
 *                                  .thenApplyAsync
 *                                  //执行线程任务  有入参、无返回值（获取维度信息进行关联，关联到对象中，不需要输出）
 *                                  .thenAcceptAsync
 *                          )
 *             将数据写入到Doris中
 */
public class DwsTradeSkuOrderWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTradeSkuOrderWindow().start(
                10029,4, Constant.DWS_T_S_O_W,Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 1.过滤空消息 并对流中数据进行类型转换 jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        if (jsonStr != null) {
                            out.collect(JSON.parseObject(jsonStr));
                        }
                    }
                }
        );
        //jsonObjDS.print();
        //{"create_time":"2024-11-24 17:25:47","sku_num":"1","split_original_amount":"9199.0000","split_coupon_amount":"0.0"
        // ,"sku_id":"18","date_id":"2024-11-24","user_id":"371","province_id":"25","sku_name":"TCL 平板电视","id":"22461",
        // "order_id":"16006","split_activity_amount":"919.9","split_total_amount":"8279.1","ts":1732440348}
        //TODO 2.按照唯一键（订单明细id）进行分组
        KeyedStream<JSONObject, String> orderDetailIdKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));
        //TODO 3.去重
        //去重方式1： 状态 + 定时器  规则是取最新（时间最大的）的数据，默认最新的数据是最全的比旧的数据  缺点：时效性差 优点：如有重复数据，向下游只传一次
        /*SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<JSONObject>("lastJsonObjState", JSONObject.class);
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        //从状态中获取上次接收到的json对象
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj == null) {
                            //说明没有重复 将当前接收到的这条json数据放到状态中，并注册5s后执行的定时器
                            lastJsonObjState.update(jsonObj);
                            long currentProcessingTime = ctx.timerService().currentProcessingTime();
                            ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000L);
                        } else {
                            //说明重复了 用当前数据的聚合时间和状态中的数据聚合时间进行比较，将时间大的放到状态中
                            //伪代码
                            String lastTs = lastJsonObj.getString("聚合时间戳");
                            String curTs = jsonObj.getString("聚合时间戳");
                            if (curTs.compareTo(lastTs) >= 0) {
                                lastJsonObjState.update(jsonObj);
                            }
                        }

                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        //当定时器被触发执行的时候，将状态中的数据发送到下游，并清除状态
                        JSONObject jsonObj = lastJsonObjState.value();
                        out.collect(jsonObj);
                        lastJsonObjState.clear();
                    }
                }
        );*/
        //去重方式2： 状态 + 抵消  优点：时效性好  缺点：如果出现重复，需要向下游传递3条数据，出现数据膨胀
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<>("lastJsonObjState", JSONObject.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        //状态中获取上次接收到的数据
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj != null) {
                            //说明重复了，将已经发送到下游的数据（状态），影响到度量值的字段进行取反再传递到下游
                            //{"create_time":"2024-11-24 17:25:47","sku_num":"1","split_original_amount":"9199.0000","split_coupon_amount":"0.0"
                            // ,"sku_id":"18","date_id":"2024-11-24","user_id":"371","province_id":"25","sku_name":"TCL 平板电视","id":"22461",
                            // "order_id":"16006","split_activity_amount":"919.9","split_total_amount":"8279.1","ts":1732440348}
                            String splitOriginalAmount = lastJsonObj.getString("split_original_amount");
                            String splitCouponAmount = lastJsonObj.getString("split_coupon_amount");
                            String splitActivityAmount = lastJsonObj.getString("split_activity_amount");
                            String splitTotalAmount = lastJsonObj.getString("split_total_amount");

                            lastJsonObj.put("split_original_amount", "-" + splitOriginalAmount);
                            lastJsonObj.put("split_coupon_amount", "-" + splitCouponAmount);
                            lastJsonObj.put("split_activity_amount", "-" + splitActivityAmount);
                            lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);
                            out.collect(lastJsonObj);
                        }
                        lastJsonObjState.update(jsonObj);
                        out.collect(jsonObj);

                    }
                }
        );
        //distinctDS.print();
        //TODO 4.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = distinctDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts") * 1000;
                                    }
                                }
                        )
        );
        //TODO 5.再次对流中数据进行类型转换 jsonObj->统计的实体类对象
        SingleOutputStreamOperator<TradeSkuOrderBean> beanDS = withWatermarkDS.map(
                new MapFunction<JSONObject, TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean map(JSONObject jsonObj) throws Exception {
                        //{"create_time":"2024-11-24 17:25:47","sku_num":"1","split_original_amount":"9199.0000","split_coupon_amount":"0.0"
                        // ,"sku_id":"18","date_id":"2024-11-24","user_id":"371","province_id":"25","sku_name":"TCL 平板电视","id":"22461",
                        // "order_id":"16006","split_activity_amount":"919.9","split_total_amount":"8279.1","ts":1732440348}
                        String skuId = jsonObj.getString("sku_id");
                        BigDecimal splitOriginalAmount = jsonObj.getBigDecimal("split_original_amount");
                        BigDecimal splitCouponAmount = jsonObj.getBigDecimal("split_coupon_amount");
                        BigDecimal splitActivityAmount = jsonObj.getBigDecimal("split_activity_amount");
                        BigDecimal splitTotalAmount = jsonObj.getBigDecimal("split_total_amount");
                        Long ts = jsonObj.getLong("ts") * 1000;
                        TradeSkuOrderBean orderBean = TradeSkuOrderBean.builder()
                                .skuId(skuId)
                                .originalAmount(splitOriginalAmount)
                                .couponReduceAmount(splitCouponAmount)
                                .activityReduceAmount(splitActivityAmount)
                                .orderAmount(splitTotalAmount)
                                .ts(ts)
                                .build();
                        return orderBean;
                    }
                }
        );
        //beanDS.print();
        //TODO 6.分组
        KeyedStream<TradeSkuOrderBean, String> skuIdKeyedDS = beanDS.keyBy(TradeSkuOrderBean::getSkuId);
        //TODO 7.开窗
        WindowedStream<TradeSkuOrderBean, String, TimeWindow> windowDS = skuIdKeyedDS.window(TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
        //TODO 8.聚合  创建聚合的骨架（数据结构，可以认为是建表），并把一些必要的信息写入
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                        //这里的decimal只能用add函数，不能用 +
                        value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                        value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                        value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        return value1;
                    }
                },
                new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>.Context context, Iterable<TradeSkuOrderBean> elements, Collector<TradeSkuOrderBean> out) throws Exception {
                        TradeSkuOrderBean orderBean = elements.iterator().next();
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        orderBean.setStt(stt);
                        orderBean.setEdt(edt);
                        orderBean.setCurDate(curDate);
                        out.collect(orderBean);
                    }
                }
        );
        //reduceDS.print();
        //TODO 9.关联sku维度
        //TODO 9.1.维度关联最基本的实现方式
        /*SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = reduceDS.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        //根据流中的对象获取要关联的维度的主键
                        String skuId = orderBean.getSkuId();
                        //根据维度的主键到Hbase维度表中获取对应的维度对象
                        //id,spu_id,price,sku_name,sku_desc,weight,tm_id,category3_id,sku_default_img,is_sale,create_time
                        JSONObject skuInfoJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, Constant.HBASE_DIMTABLE_SKU_INFO, skuId, JSONObject.class);
                        //将维度对象相关的维度属性补充到流中对象上
                        orderBean.setSkuName(skuInfoJsonObj.getString("sku_name"));
                        orderBean.setSpuId(skuInfoJsonObj.getString("spu_id"));
                        orderBean.setCategory3Id(skuInfoJsonObj.getString("category3_id"));
                        orderBean.setTrademarkId(skuInfoJsonObj.getString("tm_id"));
                        return orderBean;
                    }
                }
        );
        withSkuInfoDS.print();*/
        //TODO 9.2.优化1：旁路缓存优化 关联sku维度
        /*SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = reduceDS.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    private Connection hbaseConn;
                    private Jedis jedis;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                        jedis = RedisUtil.getJedis();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                        RedisUtil.closeJedis(jedis);
                    }

                    @Override
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        //根据流中的对象获取要关联的维度的主键
                        String skuId = orderBean.getSkuId();
                        //根据维度的主键，先从Redis中查询维度信息
                        JSONObject dimJsonObj = RedisUtil.readDim(jedis, Constant.REDIS_DIMTABLE_SKU_INFO, skuId);
                        if (dimJsonObj != null) {
                            //缓存命中 在Redis中查询到了维度信息，直接作为查询结果返回
                            System.out.println("~~~在Redis中获取到了SkuInfo的维度信息！~~~");
                        } else {
                            //在Redis中没有查询到维度信息，发送请求到HBase中查询,查询到结果，就作为查询结果返回
                            dimJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, Constant.REDIS_DIMTABLE_SKU_INFO, skuId, JSONObject.class);
                            if (dimJsonObj != null) {
                                //Hbase命中 输出打印
                                System.out.println("~~~Hbase读取到了SkuInfo的维度信息！~~~");
                                //把命中的维度结果信息缓存到Redis中
                                RedisUtil.writeDim(jedis,Constant.REDIS_DIMTABLE_SKU_INFO,skuId,dimJsonObj);
                            } else {
                                //Hbase中也没有查询到维度信息
                                System.out.println("~~~没有查询到SkuInfo的维度信息！~~~");
                            }

                        }
                        //将维度对象相关的维度属性信息，补充到流中的对象上
                        if (dimJsonObj != null) {
                            orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                            orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                            orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                            orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                        }
                        return orderBean;
                    }
                }
        );*/
        //withSkuInfoDS.print();
        //TODO 9.3.优化2：使用 旁路缓存+模版 关联维度：
        /*SingleOutputStreamOperator<TradeSkuOrderBean> withSkuDS = reduceDS.map(
                new DimMapFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                        orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                        orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                        orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                    }

                    @Override
                    public String getTableName() {
                        return Constant.HBASE_DIMTABLE_SKU_INFO;
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getSkuId();
                    }
                }
        );
        withSkuDS.print();*/
        //TODO 9.4.优化3：使用异步IO + 旁路缓存 优化
        //将异步 I/O 操作应用于 DataStream 作为 DataStream 的一次转换操作, 启用或者不启用重试
        //这四条异步关联的流，那条关联的流谁先到是没有关系的，所以这里采用无顺序的异步流函数
        /*SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = AsyncDataStream.unorderedWait(
                //指定是哪条流
                reduceDS,
                //如何发送异步请求 - 实现分发请求的 AsyncFunction
                new RichAsyncFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    private  AsyncConnection hbaseAsyncConn;
                    private StatefulRedisConnection<String,String> redisAsyncConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseAsyncConn = HBaseUtil.getHBaseAsyncConnection();
                        redisAsyncConn = RedisUtil.getRedisAsyncConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeAsyncHbaseConnection(hbaseAsyncConn);
                        RedisUtil.closeRedisAsyncConnection(redisAsyncConn);
                    }

                    @Override
                    public void asyncInvoke(TradeSkuOrderBean orderBean, ResultFuture<TradeSkuOrderBean> resultFuture) throws Exception {
                        //根据当前流的数据获取维度的主键
                        String skuId = orderBean.getSkuId();
                        //根据获取的维度主键去redis中获取维度信息
                        JSONObject dimJsonObj = RedisUtil.readDimAsync(redisAsyncConn,Constant.REDIS_DIMTABLE_SKU_INFO,skuId);
                        //如果redis获取的维度信息不为空，获取到了维度信息（维度命中），直接返回维度信息
                        if (dimJsonObj != null) {
                            System.out.println("~~~在Redis中获取到了"+Constant.REDIS_DIMTABLE_SKU_INFO+"中的"+skuId+"数据~~~");
                        } else {
                            //如果redis获取的维度信息为空，没有获取到了维度信息，根据获取到的主键去HBase中获取维度信息
                            dimJsonObj = HBaseUtil.readDimAsync(hbaseAsyncConn, Constant.HBASE_NAMESPACE, Constant.HBASE_DIMTABLE_SKU_INFO, skuId);
                            if (dimJsonObj != null) {
                                //如果HBase获取的维度信息不为空，获取到了维度信息（维度命中），直接返回维度信息
                                System.out.println("~~~在HBase中获取到了"+Constant.HBASE_NAMESPACE+"."+Constant.HBASE_DIMTABLE_SKU_INFO+"表中的"+skuId+"数据~~~");
                                //并将该维度信息写入到Redis缓存中，方便下次使用
                                RedisUtil.writeDimAsync(redisAsyncConn,Constant.REDIS_DIMTABLE_SKU_INFO,skuId,dimJsonObj);
                                System.out.println("~~~向Redis中写入了"+Constant.REDIS_DIMTABLE_SKU_INFO+"中的"+skuId+"数据~~~");
                            } else {
                                //如果HBase获取的维度信息为空，没有获取到了维度信息，输出没有该维度信息
                                System.out.println("~~~在HBase中没有"+Constant.HBASE_NAMESPACE+"."+Constant.HBASE_DIMTABLE_SKU_INFO+"表中的"+skuId+"数据！！！~~~");
                            }
                        }
                        //将获取到的维度信息，关联到当前流中
                        if (dimJsonObj != null) {
                            orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                            orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                            orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                            orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                        }
                        // 并传递到下游
                        resultFuture.complete(Collections.singleton(orderBean));

                    }
                },
                60,
                TimeUnit.SECONDS
        );
        withSkuInfoDS.print();*/
        //TODO 9.5.优化4：使用异步IO + 旁路缓存 + 模版 最终优化
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                        orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                        orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                        orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                    }

                    @Override
                    public String getTableName() {
                        return Constant.HBASE_DIMTABLE_SKU_INFO;
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getSkuId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        //withSkuInfoDS.print();
        //TODO 10.关联spu维度
        /*SingleOutputStreamOperator<TradeSkuOrderBean> withSpuInfoDS = withSkuInfoDS.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    private Connection hbaseConn;
                    private Jedis jedis;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                        jedis = RedisUtil.getJedis();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                        RedisUtil.closeJedis(jedis);
                    }

                    @Override
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        //根据流中对象获取要关联的维度的主键
                        String spuId = orderBean.getSpuId();
                        //根据维度的主键去redis中获取对应的维度信息
                        JSONObject dimJsonObj = RedisUtil.readDim(jedis, Constant.REDIS_DIMTABLE_SPU_INFO, spuId);
                        //如果从redis中获取到了维度信息，直接作为结果返回
                        if (dimJsonObj != null) {
                            //在Redis中命中该维度信息
                            System.out.println("~~~在Redis中查询到SpuInfo的维度信息！~~~");
                        } else {
                            //如果从redis中没有获取到维度信息，则根据维度主键去HBase中去获取维度信息
                            dimJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, Constant.HBASE_DIMTABLE_SPU_INFO, spuId, JSONObject.class);
                            if (dimJsonObj != null) {
                                //在hbase中命中，查询到该维度信息，直接作为结果返回
                                System.out.println("~~~在hbase中查询到SpuInfo的维度信息！~~~~");
                                //并将结果写入到Redis缓存方便下次查询使用
                                RedisUtil.writeDim(jedis, Constant.REDIS_DIMTABLE_SPU_INFO, spuId, dimJsonObj);
                            } else {
                                //如果从hbase中没有获取到维度信息，则输出 没有该维度信息
                                System.out.println("~~~没有查询到SpuInfo的维度信息！~~~");

                            }

                        }
                        //将查询的维度对象的相关属性补充到流中的对象中
                        //spu_info	dim_spu_info	info	id,spu_name,description,category3_id,tm_id
                        if (dimJsonObj != null) {
                            orderBean.setSpuName(dimJsonObj.getString("spu_name"));
                        }
                        return orderBean;
                    }
                }
        );*/
        //withSpuInfoDS.print();
        //TODO 10.1.优化1：使用异步IO + 旁路缓存 优化
        /*//将异步 I/O 操作应用于 DataStream 作为 DataStream 的一次转换操作, 启用或者不启用重试
        //这四条异步关联的流，那条关联的流谁先到是没有关系的，所以这里采用无顺序的异步流函数
        SingleOutputStreamOperator<TradeSkuOrderBean> withSpuInfoDS = AsyncDataStream.unorderedWait(
                //指定是哪条流，我们都是在上一条处理完的流的基础上补充的，所以这里的流就是上面的输出流 withSpuInfoDS
                withSkuInfoDS,
                //如何发送异步请求 - 实现分发请求的 AsyncFunction
                new RichAsyncFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    private  AsyncConnection hbaseAsyncConn;
                    private StatefulRedisConnection<String,String> redisAsyncConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseAsyncConn = HBaseUtil.getHBaseAsyncConnection();
                        redisAsyncConn = RedisUtil.getRedisAsyncConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeAsyncHbaseConnection(hbaseAsyncConn);
                        RedisUtil.closeRedisAsyncConnection(redisAsyncConn);
                    }

                    @Override
                    public void asyncInvoke(TradeSkuOrderBean orderBean, ResultFuture<TradeSkuOrderBean> resultFuture) throws Exception {
                        //根据当前流的数据获取维度的主键
                        String spuId = orderBean.getSpuId();
                        //根据获取的维度主键去redis中获取维度信息
                        JSONObject dimJsonObj = RedisUtil.readDimAsync(redisAsyncConn,Constant.REDIS_DIMTABLE_SPU_INFO,spuId);
                        //如果redis获取的维度信息不为空，获取到了维度信息（维度命中），直接返回维度信息
                        if (dimJsonObj != null) {
                            System.out.println("~~~在Redis中获取到了"+Constant.REDIS_DIMTABLE_SPU_INFO+"中的"+spuId+"数据~~~");
                        } else {
                            //如果redis获取的维度信息为空，没有获取到了维度信息，根据获取到的主键去HBase中获取维度信息
                            dimJsonObj = HBaseUtil.readDimAsync(hbaseAsyncConn, Constant.HBASE_NAMESPACE, Constant.HBASE_DIMTABLE_SPU_INFO, spuId);
                            if (dimJsonObj != null) {
                                //如果HBase获取的维度信息不为空，获取到了维度信息（维度命中），直接返回维度信息
                                System.out.println("~~~在HBase中获取到了"+Constant.HBASE_NAMESPACE+"."+Constant.HBASE_DIMTABLE_SPU_INFO+"表中的"+spuId+"数据~~~");
                                //并将该维度信息写入到Redis缓存中，方便下次使用
                                RedisUtil.writeDimAsync(redisAsyncConn,Constant.REDIS_DIMTABLE_SPU_INFO,spuId,dimJsonObj);
                                System.out.println("~~~向Redis中写入了"+Constant.REDIS_DIMTABLE_SPU_INFO+"中的"+spuId+"数据~~~");
                            } else {
                                //如果HBase获取的维度信息为空，没有获取到了维度信息，输出没有该维度信息
                                System.out.println("~~~在HBase中没有"+Constant.HBASE_NAMESPACE+"."+Constant.HBASE_DIMTABLE_SPU_INFO+"表中的"+spuId+"数据！！！~~~");
                            }
                        }
                        //将获取到的维度信息，关联到当前流中
                        if (dimJsonObj != null) {
                            orderBean.setSpuName(dimJsonObj.getString("spu_name"));
                        }
                        // 并传递到下游
                        resultFuture.complete(Collections.singleton(orderBean));

                    }
                },
                60,
                TimeUnit.SECONDS
        );
        withSpuInfoDS.print();*/
        //TODO 10.2. 使用异步IO + 旁路缓存 + 模版 最终优化
        SingleOutputStreamOperator<TradeSkuOrderBean> withSpuInfoDS = AsyncDataStream.unorderedWait(
                //注意，这里是流的接力赛，一定要换成上一个的输出流
                withSkuInfoDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setSpuName(dimJsonObj.getString("spu_name"));
                    }

                    @Override
                    public String getTableName() {
                        return Constant.HBASE_DIMTABLE_SPU_INFO;
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getSpuId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        //withSpuInfoDS.print();
        //TODO 11.关联tm维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withTmDS = AsyncDataStream.unorderedWait(
                withSpuInfoDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setTrademarkName(dimJsonObj.getString("tm_name"));
                    }

                    @Override
                    public String getTableName() {
                        return Constant.HBASE_DIMTABLE_TRADEMARK;
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getTrademarkId();
                    }
                },
                60,
                TimeUnit.SECONDS


        );
        //withTmDS.print();
        //TODO 12.关联category3维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withc3Stream = AsyncDataStream.unorderedWait(
                withTmDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setCategory3Name(dimJsonObj.getString("name"));
                        orderBean.setCategory2Id(dimJsonObj.getString("category2_id"));
                    }

                    @Override
                    public String getTableName() {
                        return Constant.HBASE_DIMTABLE_D_B_C3;
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getCategory3Id();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        //withc3Stream.print();
        //TODO 13.关联category2维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withc2Stream = AsyncDataStream.unorderedWait(
                withc3Stream,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setCategory2Name(dimJsonObj.getString("name"));
                        orderBean.setCategory1Id(dimJsonObj.getString("category1_id"));
                    }

                    @Override
                    public String getTableName() {
                        return Constant.HBASE_DIMTABLE_D_B_C2;
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getCategory2Id();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        //withc2Stream.print();
        //TODO 14.关联category1维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withc1Stream = AsyncDataStream.unorderedWait(
                withc2Stream,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setCategory1Name(dimJsonObj.getString("name"));
                    }

                    @Override
                    public String getTableName() {
                        return Constant.HBASE_DIMTABLE_D_B_C1;
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getCategory1Id();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        withc1Stream.print();
        //TODO 15.将关联结果写入Doris
        withc1Stream
                //把当前流中的对象转换成json字符串
                .map(new BeanToJsonStrMapFunction<>())
                //写入到Doris中
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_TABLE_T_S_O_W));


    }
}
