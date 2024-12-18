package com.atguigu.gmall.realtime.dwd.log.split;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.checkerframework.checker.units.qual.A;

import java.util.HashMap;
import java.util.Map;

/**
 * @author shichunjian
 * @create date 2024-10-28 15:31
 * @Description：日志分流
 * 需要启动的进程
 *  jar数据源 flume zk hadoop maxwell kafka flink
 *
 *  kafkaSource:从kafka主题中写入数据，也可以保证写入数据的精准一次，需要如下操作
 *          开启检查点
 *          .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
 *          .setTransactionlIdPrefix("dwd_base_log_")
 *          .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"")
 *          在消费端，需要设置消费的隔离级别为读已提交 .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed")
 */

public class DwdBaseLog extends BaseApp {
    private final String START = "start";
    private final String ERR = "err";
    private final String DISPLAY = "display";
    private final String ACTION = "action";
    private final String PAGE = "page";
    public static void main(String[] args) throws Exception {
        new DwdBaseLog().start(10011,4,"dwd_base_log", Constant.TOPIC_LOG);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 对流中数据类型进行转换 并做简单的ETL
        SingleOutputStreamOperator<JSONObject> jsonObjDS = etl(kafkaStrDS);

        //TODO  对新老访客标记进行修复
        SingleOutputStreamOperator<Object> fixedDS = fixedNewAndOld(jsonObjDS);

        //TODO 分流 错误日志-错误侧输出流 启动日志-启动侧输出流 曝光日志-曝光侧输出流 动作日志-动作侧输出流  页面日志-主流
        Map<String, DataStream<String>> streamMap = splitStream(fixedDS);


        //TODO 将不同流的数据写入到kafka的不同的主题中
        writeToKafka(streamMap);
    }

    private void writeToKafka(Map<String, DataStream<String>> streamMap) {
        streamMap.get(PAGE).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        streamMap.get(ERR).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        streamMap.get(START).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        streamMap.get(DISPLAY).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        streamMap.get(ACTION).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
    }

    private Map<String, DataStream<String>> splitStream(SingleOutputStreamOperator<Object> fixedDS) {
        //定义侧输出流标签
        OutputTag<String> errTag = new OutputTag<String>("errTag"){};
        OutputTag<String> startTag = new OutputTag<String>("startTag"){};
        OutputTag<String> displayTag = new OutputTag<String>("displayTag"){};
        OutputTag<String> actionTag = new OutputTag<String>("actionTag"){};
        //分流
        // 假设 fixedDS 是 DataStream<Object> 类型，使用 map 将其转换为 DataStream<JSONObject>
        DataStream<JSONObject> jsonObjectDS = fixedDS.map(value -> {
            // 确保 value 是 JSONObject 类型
            if (value instanceof JSONObject) {
                return (JSONObject) value;
            }
            // 处理异常情况
            throw new IllegalArgumentException("输入不是一个json对象 JSONObject");
        });

        SingleOutputStreamOperator<String> pageDS = jsonObjectDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<String> out) throws Exception {
                        // ~~~错误日志~~~
                        JSONObject errJsonObj = jsonObj.getJSONObject("err");
                        if (errJsonObj != null) {
                            // 将错误日志写到错误侧输出流
                            ctx.output(errTag, jsonObj.toJSONString());
                            jsonObj.remove("err");
                        }

                        JSONObject startJsonObj = jsonObj.getJSONObject("start");
                        if (startJsonObj != null) {
                            // ~~~启动日志~~~
                            // 将启动日志写到启动侧输出流
                            ctx.output(startTag, jsonObj.toJSONString());
                        } else {
                            // ~~~页面日志~~~
                            JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                            JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                            Long ts = jsonObj.getLong("ts");

                            // ~~~曝光日志~~~
                            JSONArray displayArr = jsonObj.getJSONArray("displays");
                            if (displayArr != null && displayArr.size() > 0) {
                                // 遍历当前页面的所有曝光信息
                                for (int i = 0; i < displayArr.size(); i++) {
                                    JSONObject displayJsonObj = displayArr.getJSONObject(i);
                                    // 定义一个新的JSON对象，用于封装遍历出来的曝光数据
                                    JSONObject newDisplayJsonObj = new JSONObject();
                                    newDisplayJsonObj.put("common", commonJsonObj);
                                    newDisplayJsonObj.put("page", pageJsonObj);
                                    newDisplayJsonObj.put("display", displayJsonObj);
                                    newDisplayJsonObj.put("ts", ts);
                                    // 将曝光日志写到曝光侧输出流
                                    ctx.output(displayTag, newDisplayJsonObj.toJSONString());
                                }
                                jsonObj.remove("displays");
                            }

                            // ~~~动作日志~~~
                            JSONArray actionArr = jsonObj.getJSONArray("actions");
                            if (actionArr != null && actionArr.size() > 0) {
                                // 遍历出每一个动作
                                for (int i = 0; i < actionArr.size(); i++) {
                                    JSONObject actionJsonObj = actionArr.getJSONObject(i);
                                    // 定义一个新的JSON对象，用于封装动作信息
                                    JSONObject newActionJsonObj = new JSONObject();
                                    newActionJsonObj.put("common", commonJsonObj);
                                    newActionJsonObj.put("page", pageJsonObj);
                                    newActionJsonObj.put("action", actionJsonObj);
                                    // 将动作日志写到动作侧输出流
                                    ctx.output(actionTag, newActionJsonObj.toJSONString());
                                }
                                jsonObj.remove("actions");
                            }

                            // 页面日志写到主流中
                            out.collect(jsonObj.toJSONString());
                        }
                    }
                }
        );


        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errTag);
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        pageDS.print("页面：");
        errDS.print("错误：");
        startDS.print("启动：");
        displayDS.print("曝光：");
        actionDS.print("动作：");

        Map<String, DataStream<String>> streamMap = new HashMap<>();
        streamMap.put(ERR,errDS);
        streamMap.put(START,startDS);
        streamMap.put(DISPLAY,displayDS);
        streamMap.put(ACTION,actionDS);
        streamMap.put(PAGE,pageDS);
        return streamMap;
    }

    private static SingleOutputStreamOperator<Object> fixedNewAndOld(SingleOutputStreamOperator<JSONObject> jsonObjDS) {
        //按照设备id进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        //使用Flink的状态编程完成修复
        SingleOutputStreamOperator<Object> fixedDS = keyedDS.map(
                new RichMapFunction<JSONObject, Object>() {
                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<String>("lastVisitDateState", String.class);
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);

                    }
                    @Override
                    public JSONObject map(JSONObject jsonObj) throws Exception {
                        //获取isNew的值
                        String isNew = jsonObj.getJSONObject("common").getString("is_new");
                        //从状态中获取首次访问日期
                        String lastVisitDate = lastVisitDateState.value();
                        //获取当前访问日期
                        Long ts = jsonObj.getLong("ts");
                        String curVisDate = DateFormatUtil.tsToDate(ts);
                        if ("1".equals(isNew)) {
                            // 如果is_new的值为1
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                //如果键控状态为null，认为本次是该方可首次访问APP,将日志中ts对应的日期更新到状态中，不对is_new字段做修改；
                                lastVisitDateState.update(curVisDate);
                            } else {
                                //键控状态不为null，目首次访间目期不是当日，说明访问的是老访客，将is_new字段置为:
                                if (!lastVisitDate.equals(curVisDate)){
                                    isNew = "0";
                                    jsonObj.getJSONObject("common").put("is_new",isNew);
                                }
                                //如果键控状态不为null，且首次访间日期是当日，说明访间的是新访客，不做操作:
                            }
                        } else {
                            // 如果is_new的值为0
                            //如果键控状态为n，说明访问APP的是老访客但本次是该访客的页面日志首次进入程序。当前端新老访客状念标记丢失时,
                            //日志进入程序被判定为新访客，FLinK程序就可以纠正被误判的访客状态标记，只要将状态中的日期设置为今天之前即可。本程选择将状态更新为昨日
                            if (StringUtils.isEmpty(lastVisitDate)){
                                String yesterDay = DateFormatUtil.tsToDate(ts -24*60*60*1000);
                                lastVisitDateState.update(yesterDay);
                            }
                        }
                        return jsonObj;
                    }
                }
        );
        //fixedDS.print();
        return fixedDS;
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDS) {
        //定义侧输出流标签
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag"){};
        //ETL
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>(){
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            //如果转换的是时候，没有发生异常，说明是标准的json，将数据传递到下游
                            out.collect(jsonObj);
                        } catch (Exception e) {
                            //如果转换的是时候，发生了异常，说明不是标准的json，属于脏数据，将数据输出到侧输出流中
                            ctx.output(dirtyTag, jsonStr);
                        }
                    }
                }
        );
        //jsonObjDS.print("标准的json：");
        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        //dirtyDS.print("脏数据：");

        //将侧输出流中的脏数据写入到kafka主题中
        KafkaSink<String> kafkaSink = FlinkSinkUtil.getKafkaSink("dirty_data");
        dirtyDS.sinkTo(kafkaSink);
        return jsonObjDS;
    }
}
