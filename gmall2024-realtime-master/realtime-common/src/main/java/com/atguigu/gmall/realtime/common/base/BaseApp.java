package com.atguigu.gmall.realtime.common.base;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import lombok.val;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.logging.Handler;

/**
 * @author shichunjian
 * @create date 2024-10-28 10:12
 * @Description：FlinkAPI应用程序的基类
 * 模板方法设置模式：在父类中定义完成某一个功能的核心算法骨架（步骤），有些步骤在父类中没有办法实现，需要延迟到子类中去完成
 *  好处：预定了模板（可复用，而且统一）
 *      在不改变父类核心算法骨架的前提下，每个子类都可以有自己不同的实现
 */
public abstract class BaseApp {
    public void start(int port, int parallelism, String ckAndGroupId, String topic) throws Exception {
        //TODO 1.基本环境配置
        //1.1 指定流处理环境
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //1.2 设置并行度
        env.setParallelism(parallelism);
        //TODO 2.检查点相关配置
        //2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        /*//2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.3 设置job取消后检查点是否保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置两个检查点之间最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(4000L);
        //2.5 设置重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));
        //2.6 设置状态后端以及检查点存储路径
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://bigdata.sbx.com:8020/ck/" + ckAndGroupId);
        //2.7 设置操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME","hdfs");*/
        //TODO 3.从kafka的topic_db主题中读取业务数据
        //3.1 声明消费者主题以及消费者组
        //3.2 创建消费者对象
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(topic, ckAndGroupId);
        //3.3 消费数据 封装为流
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");
        //TODO 4.处理逻辑
        handle(env,kafkaStrDS);
        //TODO 5.提交作业
        env.execute();
    }

    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS);

}