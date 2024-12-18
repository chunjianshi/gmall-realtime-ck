package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.DimJoinFunction;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;


/**
 * @author shichunjian
 * @create date 2024-11-26 16:23
 * @Description：维度关联Redis旁路缓存优化后的模版抽取
 */
public abstract class DimMapFunction<T> extends RichMapFunction<T,T> implements DimJoinFunction<T> {
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
    public T map(T obj) throws Exception {
        //根据流中对象获取要关联的维度的主键
        String key = getRowKey(obj);
        //根据维度的主键去redis中获取对应的维度信息
        JSONObject dimJsonObj = RedisUtil.readDim(jedis, getTableName(), key);
        //如果从redis中获取到了维度信息，直接作为结果返回
        dimJsonObj = RedisUtil.readDim(jedis, getTableName(), key);
        if (dimJsonObj != null) {
            //在Redis中命中该维度信息
            System.out.println("~~~从Redis中获取到了"+getTableName()+"表的"+key+"数据~~~");
        } else {
            //如果从redis中没有获取到维度信息，则根据维度主键去HBase中去获取维度信息
            dimJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, getTableName(), key, JSONObject.class);
            if (dimJsonObj != null) {
                //在hbase中命中，查询到该维度信息，直接作为结果返回
                System.out.println("~~~从Hbase中获取到了"+getTableName()+"表的"+key+"数据~~~");
                //并将结果写入到Redis缓存方便下次查询使用
                RedisUtil.writeDim(jedis,getTableName(),key,dimJsonObj);
            } else {
                //如果从hbase中没有获取到维度信息，则输出 没有该维度信息
                System.out.println("~~~没有找到"+getTableName()+"表的"+key+"的数据！~~~");
            }
        }
        //将查询的维度对象的相关属性补充到流中的对象中
        if (dimJsonObj != null) {
            addDims(obj,dimJsonObj);
        }
        return obj;
    }
/*    public abstract void addDims(T obj, JSONObject dimJsonObj);
    public abstract String getTableName() ;  //只有声明，没有实现方法
    public abstract String getRowKey(T obj); //只有声明，没有实现方法*/
}
