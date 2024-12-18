package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TrafficPageViewBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import javax.security.auth.login.Configuration;

/**
 * @author shichunjian
 * @create date 2024-11-22 8:00
 * @Description：按照版本、地区、渠道、新老访客对pv、uv、sv、dur进行聚合统计
 * 会话数sv、页面浏览数pv、浏览总时长dur_sum、独立访客uv
 * vc 版本号 ch 渠道 ar 地区 isNEW 新老访客状态标记
 * 需要启动的进程：zk kafka flume doris 日志分流DwdBaseLog DwsTrafficVcChArIsNewPageViewWindow
 */
public class DwsTrafficVcChArIsNewPageViewWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTrafficVcChArIsNewPageViewWindow().start(
                10022,
                4,
                Constant.DWS_TRAFFIC_V_C_A_I_N_P_V_W,
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );

    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 1.对流中数据进行类型转换 jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);
        //TODO 2.按照mid设备id对流中数据进行分组（计算独立访客UV）
        KeyedStream<JSONObject, String> midKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        //TODO 3.再次对流中数据进行类型转换，jsonObj ->统计的实体类对象
        SingleOutputStreamOperator<TrafficPageViewBean> beanDS = midKeyedDS.map(
                new RichMapFunction<JSONObject, TrafficPageViewBean>() {
                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor =
                        new ValueStateDescriptor<String>("lastVisitDateState", String.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }
                    @Override
                    public TrafficPageViewBean map(JSONObject jsonObj) throws Exception {
                        JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                        JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                        //从状态中获取当前配置上次访问日期
                        String lastVisitDate = lastVisitDateState.value();
                        //获取当前访问日期
                        Long ts = jsonObj.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);
                        Long uvCt = 0L;
                        if(StringUtils.isEmpty(lastVisitDate) || !lastVisitDate.equals(curVisitDate)){
                            uvCt = 1L;
                            lastVisitDateState.update(curVisitDate);
                        }

                        String lastPageId = pageJsonObj.getString("last_page_id");

                        Long svCt = StringUtils.isEmpty(lastPageId) ? 1L : 0L;
                        return new TrafficPageViewBean(
                                "",
                                "",
                                "",
                                commonJsonObj.getString("vc"),
                                commonJsonObj.getString("ch"),
                                commonJsonObj.getString("ar"),
                                commonJsonObj.getString("is_new"),
                                uvCt,
                                svCt,
                                1L,
                                pageJsonObj.getLong("during_time"),
                                ts
                        );
                    }
                }
        );
        //beanDS.print();
        //TODO 4.指定Watermark以及提取事件时间字段  用于触发计算时间
        SingleOutputStreamOperator<TrafficPageViewBean> withWatermarkDS = beanDS.assignTimestampsAndWatermarks(
                //WatermarkStrategy.forBoundedOutOfOrderness()  水位线 有阶乱序
                //这里使用单调递增 按照时间顺序的水位线
                WatermarkStrategy
                        .<TrafficPageViewBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TrafficPageViewBean>() {
                                    @Override
                                    public long extractTimestamp(TrafficPageViewBean bean, long recordTimestamp) {
                                        return bean.getTs();
                                    }
                                }
                        )
        );
        //TODO 5.分组--按照统计的维度进行分组
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> dimKeyedDS = withWatermarkDS.keyBy(
                new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(TrafficPageViewBean bean) throws Exception {
                        return Tuple4.of(bean.getVc(),   // 版本号
                                bean.getCh(),   // 渠道
                                bean.getAr(),   // 地区
                                bean.getIsNew() // 新老访客标记
                        );
                    }
                }
        );
        //TODO 6.开窗
        //以滚动事件时间窗口为例，分析如下几个窗口相关的问题
        //窗口对象时间创建:当属于这个窗口的第一个元素到来的时候创建窗口对象
        //窗口的其实结束时间（窗口为什么是左闭右开的）
        //向下取整
        //窗口什么时间触发计算 窗口时间+乱序时间=触发计算时间(写死的) 水位线  水位线是一直在变化的  水位线 => 触发计算时间 开始触发计算
        //窗口什么时候关闭  窗口时间+乱序时间+允许迟到时间 = 触发计算时间(写死的) +允许迟到时间 = 关窗时间  水位线=> 关窗时间
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowDS
                = dimKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
        //TODO 7.聚合计算
        SingleOutputStreamOperator<TrafficPageViewBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TrafficPageViewBean>() {
                    //得到聚合结果，但是时间是空的
                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                        value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                        return value1;
                    }
                },
                //取聚合的时间
                new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {
                        TrafficPageViewBean pageViewBean = input.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        pageViewBean.setStt(stt);
                        pageViewBean.setEdt(edt);
                        pageViewBean.setCur_date(curDate);
                        out.collect(pageViewBean);

                    }
                }
        );
        reduceDS.print();
        //TODO 8.将聚合的结果写入到Doris表
        reduceDS
                //在向Doris写数据前，将流中统计的实体类对象转成为json格式字符串
                .map(new BeanToJsonStrMapFunction<TrafficPageViewBean>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRAFFIC_V_C_A_I_N_P_V_W));
    }
}
