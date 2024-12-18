package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.DimJoinFunction;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author shichunjian
 * @create date 2024-11-27 10:12
 * @Description： 发送 异步请求+旁路缓存 进行维度关联的模板类
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T> {
    private AsyncConnection hbaseAsyncConn;
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
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        //创建异步编排对象 执行线程任务，有返回值
        //因为我们的这里的一个线程的结果输出，会作为下一个结果的数据，来回接力补充维度信息，所以是有输入和输出的
        CompletableFuture.supplyAsync(
                new Supplier<JSONObject>() {
                    @Override
                    public JSONObject get() {
                        //根据当前流的数据获取维度的主键
                        String key = getRowKey(obj);
                        //根据获取的维度主键去redis中获取维度信息
                        JSONObject dimJsonObj = RedisUtil.readDimAsync(redisAsyncConn, getTableName(), key);
                        return dimJsonObj;
                    }
                }
        //有入参、有返回值，各线程独立，上一个线程任务的返回值作为当前任务的入参
        ).thenApplyAsync(
                new Function<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject apply(JSONObject dimJsonObj) {
                        //如果redis获取的维度信息不为空，获取到了维度信息（维度命中），直接返回维度信息
                        if (dimJsonObj != null) {
                            System.out.println("~~~在Redis中获取到了"+getTableName()+"中的"+getRowKey(obj)+"数据~~~");
                        } else {
                            //如果redis获取的维度信息为空，没有获取到了维度信息，根据获取到的主键去HBase中获取维度信息
                            dimJsonObj = HBaseUtil.readDimAsync(hbaseAsyncConn, Constant.HBASE_NAMESPACE, getTableName(), getRowKey(obj));
                            //如果HBase获取的维度信息不为空，获取到了维度信息（维度命中），直接返回维度信息
                            if (dimJsonObj != null) {
                                System.out.println("~~~在HBase中获取到了"+Constant.HBASE_NAMESPACE+"."+getTableName()+"表中的"+getRowKey(obj)+"数据~~~");
                                //并将该维度信息写入到Redis缓存中，方便下次使用
                                RedisUtil.writeDimAsync(redisAsyncConn,getTableName(),getRowKey(obj),dimJsonObj);
                                System.out.println("~~~向Redis中写入了"+getTableName()+"中的"+getRowKey(obj)+"数据~~~");
                            } else {
                                //如果HBase获取的维度信息为空，没有获取到了维度信息，输出没有该维度信息
                                System.out.println("~~~在HBase中没有"+Constant.HBASE_NAMESPACE+"."+getTableName()+"表中的"+getRowKey(obj)+"数据！！！~~~");
                            }
                        }
                        return dimJsonObj;
                    }
                }
                //有入参，无返回值 这里不需要返回值，只需要有入参即可，所以选择 thenAcceptAsync 函数
        ).thenAcceptAsync(
                new Consumer<JSONObject>() {
                    @Override
                    public void accept(JSONObject dimJsonObj) {
                        //如果获取到了维度信息（不为空），就将获取到的维度信息，关联到当前流中
                        if (dimJsonObj != null) {
                            addDims(obj,dimJsonObj);
                        }
                        // 并传递到下游
                        resultFuture.complete(Collections.singleton(obj));
                    }
                }
        );
    }
}
