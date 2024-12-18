package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TradeProvinceOrderBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.gmall.realtime.common.function.DimAsyncFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
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
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.naming.CompositeName;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author shichunjian
 * @create date 2024-11-27 17:52
 * @Description：省份力度下单聚合统计
 * 需要启动的进程：maxwell kafka zk hdfs hbase doris redis 订单明细表的程序DwdTradeOrderDetail DwsTradeProvinceOrderWindow
 */
public class DwsTradeProvinceOrderWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTradeProvinceOrderWindow().start(
                10020,
                4,
                Constant.TOPIC_DWS_T_P_O_W,
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );

    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 1.过来的订单明细表 通过 过滤空消息 并对订单明细表的流中数据进行类型转换 jsonStr->jsonObj（就是订单明细表的对象）
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        if (jsonStr != null) {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            out.collect(jsonObj);
                        }

                    }
                }
        );
        //需要打印输出，看看是什么样子格式的，才能判断进行下一步操作！就是订单明细表的对象jsonObj 过滤处理后的结果赋值给了 jsonObjDS，可以打印输出看
        //jsonObjDS.print();
        //{"create_time":"2024-11-24 18:16:57","sku_num":"1","split_original_amount":"488.0000","split_coupon_amount":"0.0",
        // "sku_id":"33","date_id":"2024-11-24","user_id":"5174","province_id":"24","sku_name":"香奈儿（Chanel）35ml",
        // "id":"30408","order_id":"21717","split_activity_amount":"0.0","split_total_amount":"488.0","ts":1732702617}
        //TODO 2.过来的订单明细表通过 按照唯一键（订单明细id）进行分组 好用于下面的状态方法去重 订单明细表的对象jsonObj 过滤处理后处理后，又进行了分组，结果赋值给了orderDetailIdKeyedDS
        KeyedStream<JSONObject, String> orderDetailIdKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));
        //TODO 3.过来的订单明细表通过 状态中去重 方法去重  订单明细表的对象jsonObj 过滤处理后处理后，又进行了分组，又进行了去重，结果赋值给了 distinctDS
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    //状态
                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //新的状态描述器方法
                        ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<>("lastJsonObjState", JSONObject.class);
                        //状态保留时间 10秒
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                        //更新状态
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        //获取状态中的值
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        //根据获取状态的值，判断是否重复
                        if (lastJsonObj != null) {
                            //数据重复 需要先从状态中获取影响的字段的数据，然后进行取反操作，本身取反后再传递到状态中，这样就抵消了！
                            String splitTotalAmount = lastJsonObj.getString("split_total_amount");
                            //本身取反
                            lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);
                            //传递传递抵消的值，这样状态中就抵消了！
                            out.collect(lastJsonObj);
                        }
                        //最新的数据放到状态中
                        lastJsonObjState.update(jsonObj);
                        //传递最新的值到状态
                        out.collect(jsonObj);

                    }
                }

        );
        //distinctDS.print();
        //TODO 4.对去重后的订单明细表 输出的对象distinctDS 订单明细表的数据去重后，添加指定Watermark以及提取事件时间字段
        //订单明细表的对象jsonObj 过滤处理后处理后，又进行了分组，又进行了去重,又进行了添加指定Watermark以及提取事件时间字段 后赋值给了 watermarkDS
        SingleOutputStreamOperator<JSONObject> watermarkDS = distinctDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    // "split_total_amount":"488.0","ts":1732702617}
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        //提取的时间需要转化成毫秒
                                        return jsonObj.getLong("ts") * 1000;
                                    }
                                }
                        )
        );
        //TODO 5.对 添加watermark的订单明细表 与 目标对象 TradeProvinceOrderBean 进行关联 或者说 进行类型转换 jsonObj->统计的实体类对象（统一在bean中严格定义）
        //订单明细表的对象jsonObj 过滤处理后处理后，又进行了分组，又进行了去重,又进行了添加指定Watermark以及提取事件时间字段 后 ,开始和目标表TradeProvinceOrderBean进行关联
        //这些操作都是在水位线里面进行操作的 watermarkDS.map  把每个水位线执行后的结果，赋值给 beanDS，现在这里是真正的目标结果的对象了，再往下就是目标的结果对象和维度表hbase的关联了！
        SingleOutputStreamOperator<TradeProvinceOrderBean> beanDS = watermarkDS.map(
                //就是  kafka中过来的订单明细表 和 HBase过来的省份的表  关联后的结果表的 字段的格式及字段的类型，在这里进行严格的定义！
                //定义完成后，先和 kafka 过来的订单明细表,进行关联，尽可能的关联上所有信息
                //MapFunction<JSONObject kafka的订单明细表作为输入类型指定的是JSONObject对象, TradeProvinceOrderBean 关联后的结果表作为输出json对象>
                new MapFunction<JSONObject, TradeProvinceOrderBean>() {

                    @Override
                    //把上述梳理后的订单明细表的输入对象 jsonObj传递过去
                    public TradeProvinceOrderBean map(JSONObject jsonObj) throws Exception {
                        //{"create_time":"2024-11-24 18:16:57","sku_num":"1","split_original_amount":"488.0000","split_coupon_amount":"0.0",
                        // "sku_id":"33","date_id":"2024-11-24","user_id":"5174","province_id":"24","sku_name":"香奈儿（Chanel）35ml",
                        // "id":"30408","order_id":"21717","split_activity_amount":"0.0","split_total_amount":"488.0","ts":1732702617}
                        //通过订单明细表jsonObj的省份的province_id 字段 获取到 省份的id信息
                        String provinceId = jsonObj.getString("province_id");
                        //通过订单明细表jsonObj的累计下单金额 split_total_amount  字段 获取到 金额的具体数据信息
                        BigDecimal splitTotalAmount = jsonObj.getBigDecimal("split_total_amount");
                        //通过订单明细表jsonObj的下单时间的时间戳 ts  字段 获取到 下单时间的具体数据信息
                        Long ts = jsonObj.getLong("ts");
                        //通过订单明细表jsonObj的 订单order_id 字段 获取到 订单id的具体信息
                        String orderId = jsonObj.getString("order_id");
                        //Set idSet = new HashSet();
                        //idSet.add(orderId);
                        //把获取到的这些 订单明细表jsonObj的数据信息，关联到 结果表TradeProvinceOrderBean的对应的字段上
                        TradeProvinceOrderBean orderBean = TradeProvinceOrderBean.builder()
                                //把订单明细表jsonObj的省份数据关联传递到TradeProvinceOrderBean结果表provinceId 字段上
                                .provinceId(provinceId)
                                //把订单明细表jsonObj的累计下单金额 关联传递到TradeProvinceOrderBean结果表orderAmount 字段上
                                .orderAmount(splitTotalAmount)
                                //把订单明细表jsonObj的订单order_id 字段的值，通过 HashSet去重后 关联传递到TradeProvinceOrderBean结果表orderIdSet 字段上
                                .orderIdSet(new HashSet<>(Collections.singleton(orderId)))
                                //把订单明细表jsonObj的下单时间 关联传递到TradeProvinceOrderBean结果表ts 字段上
                                .ts(ts)
                                //执行关联构建操作
                                .build();
                        //把关联后的结果数据，返回给TradeProvinceOrderBean的对象orderBean
                        return orderBean;
                    }
                }
        );
        //beanDS.print();
        //TODO 6.分组 对已经和订单明细表关联后的结果表 beanDS对象，我们按照省份的id进行分组，分组后我们就可以直接和维度表的维度进行直接关联了，这样我们一关联就是一组，不用一个一个关联了！
        KeyedStream<TradeProvinceOrderBean, String> provinceIdKeyedDS = beanDS.keyBy(TradeProvinceOrderBean::getProvinceId);
        //TODO 7.开窗 对上面已经分好组的provinceIdKeyedDS 的数据，我们按照10秒一个滚动窗口进行开窗（上面是按照id分组，这里是按照时间分组）
        WindowedStream<TradeProvinceOrderBean, String, TimeWindow> windowDS = provinceIdKeyedDS.window(TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
        //TODO 8.聚合  创建聚合的骨架（数据结构，可以认为是建表），并把一些必要的信息写入
        //对于上面已经和订单明细表关联后的结果表按照按照id省份分组，按照时间分组（开窗）后，我们把开窗的一些的时间属性 开窗的开始时间、结束时间、当前日期 还有 当前窗口内的订单数量进行聚合统计的结果 算出来，并关联到结果表上
        //并把最终的10S时间窗口内的聚合结果赋值给 reduceDS
        SingleOutputStreamOperator<TradeProvinceOrderBean> reduceDS = windowDS.reduce(
                //10S窗口的聚合操作
                new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    //value1 是上一次的聚合结果 value2 是当前被处理的数据元素 将 value1 和 value2 进行合并（即通过 reduce 方法指定的逻辑），并将结果返回，作为下一次聚合的 value1
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) throws Exception {
                        //把10S窗口内的订单金额进行聚合
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        //把10S窗口内的订单ID添加到已有的集合中 将 value2 的订单 ID 集合合并到 value1 的订单 ID 集合中
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        //将聚合后的结果返回
                        return value1;
                    }
                },
                //窗口的元数据处理 WindowFunction 访问窗口的元数据（如窗口的开始和结束时间）并处理整个窗口的数据
                //这里的窗口都已经是按照省份的 id 和 时间窗口分组了，一个窗口的聚合后的数据，只有 一个省的ID和 同一个开始时间和结束结束时间
                new WindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderBean> input, Collector<TradeProvinceOrderBean> out) throws Exception {
                        //从窗口关联后的结果表的数据中第一个元素
                        TradeProvinceOrderBean orderBean = input.iterator().next();
                        //窗口的开始时间
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        //窗口的结束时间
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        //窗口日期 天
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        //针对第一个元素数据赋值 开始时间
                        orderBean.setStt(stt);
                        //针对第一个元素数据赋值 结束时间
                        orderBean.setEdt(edt);
                        //针对第一个元素数据赋值 当前日期
                        orderBean.setCurDate(curDate);
                        //针对第一个元素数据赋值 当前窗口的聚合订单数量
                        orderBean.setOrderCount((long) orderBean.getOrderIdSet().size());
                        //针对第一个元素数据赋值 聚合后的结果，传递到下游
                        out.collect(orderBean);
                    }
                }
        );
        //reduceDS.print();
        //TODO 9.关联省份维度 异步IO+旁路缓存+模板  TradeProvinceOrderBean orderBean
        SingleOutputStreamOperator<TradeProvinceOrderBean> withProvinceDS = AsyncDataStream.unorderedWait(
                //传入输入流，上次的输出作为输入
                reduceDS,
                //调用自己封装的 读取维度信息的函数 DimAsyncFunction 异步IO+旁路缓存+模板
                new DimAsyncFunction<TradeProvinceOrderBean>() {
                    @Override
                    //id,name,region_id,area_code,iso_code,iso_3166_2
                    public void addDims(TradeProvinceOrderBean orderBean, JSONObject dimJsonObj) {
                        //TradeProvinceOrderBean(stt=2024-11-28 10:13:10, edt=2024-11-28 10:13:20, curDate=2024-11-28, provinceId=29, provinceName=, orderCount=1, orderAmount=8197.0, ts=1732760000, orderIdSet=[22850])
                        //从上面的输出数据来看，这里也就只差 省份的名称没有关联了！
                        //去缓存或者HBase中，查找 gmall:dim_base_province 下的省份的 ID对应的 省份的名称，赋值给 provinceName
                        orderBean.setProvinceName(dimJsonObj.getString("name"));

                    }

                    @Override
                    public String getTableName() {
                        return Constant.HBASE_DIMTABLE_PROVINCE;
                    }

                    @Override
                    public String getRowKey(TradeProvinceOrderBean orderBean) {
                        return orderBean.getProvinceId();
                    }
                }, 60,
                TimeUnit.SECONDS

        );
        //withProvinceDS.print();
        //TradeProvinceOrderBean(stt=2024-11-28 11:19:50, edt=2024-11-28 11:20:00, curDate=2024-11-28, provinceId=26, provinceName=广东, orderCount=2, orderAmount=11788.0, ts=1732763999, orderIdSet=[23031, 23027])
        //至此，数据全了，可以写入了！
        //TODO 10.将关联结果写入Doris
        withProvinceDS
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_TABLE_T_P_O_W));
    }
}
