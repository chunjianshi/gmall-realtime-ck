package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.CartAddUuBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.swing.text.EditorKit;

/**
 * @author shichunjian
 * @create date 2024-11-24 12:48
 * @Description：
 * 启动zk、kafka、HDFS、Maxwell、Doris  运行DwdTradeCartAdd、DwsTradeCartAddUuWindow
 */
public class DwsTradeCartAddUuWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTradeCartAddUuWindow().start(
                10026,4, Constant.DWS_T_C_A_U_W,Constant.TOPIC_DWD_TRADE_CART_ADD
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 1.对流中的数据类型进行转换 jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);
        //jsonObjDS.print();
        //{"sku_num":"1","user_id":"3315","sku_id":"9","id":"12003","ts":1732281998}
        //TODO 2.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
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
        //TODO 3.按照用户的id进行分组
        KeyedStream<JSONObject, String> keyedDS
                = withWatermarkDS.keyBy(jsonObj -> jsonObj.getString("user_id"));
        //TODO 4.使用Flink的状态编程 判断是否为架构独立用户 这里不需要封装统计的实体类对象，直接将jsonObj传递到下游
        SingleOutputStreamOperator<JSONObject> cartUUDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<String> lastCartDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastCartDateState", String.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastCartDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        //从状态中获取上次加购日期
                        String lastCartDate = lastCartDateState.value();
                        //获取当前这次加购日期
                        Long ts = jsonObj.getLong("ts") * 1000;
                        String curCartDate = DateFormatUtil.tsToDate(ts);
                        if (org.apache.commons.lang3.StringUtils.isEmpty(lastCartDate) || !lastCartDate.equals(curCartDate)) {
                            out.collect(jsonObj);
                            lastCartDateState.update(curCartDate);
                        }

                    }
                }
        );
        //TODO 5.开窗
        AllWindowedStream<JSONObject, TimeWindow> windowDS
                = cartUUDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
        //TODO 6.聚合
        SingleOutputStreamOperator<CartAddUuBean> aggregateDS = windowDS.aggregate(
                new AggregateFunction<JSONObject, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(JSONObject value, Long accumulator) {
                        return ++accumulator;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return null;
                    }
                },
                new AllWindowFunction<Long, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Long> values, Collector<CartAddUuBean> out) throws Exception {
                        Long cartUUCt = values.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        out.collect(new CartAddUuBean(
                                stt,
                                edt,
                                curDate,
                                cartUUCt
                        ));
                    }
                }
        );
        //TODO 7.将聚合结果写到Doris
        aggregateDS.print();
        aggregateDS
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_T_C_A_U_W));

    }
}

