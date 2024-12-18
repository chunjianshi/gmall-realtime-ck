package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.constant.Constant;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.security.Key;
import java.util.concurrent.ExecutionException;

/**
 * @author shichunjian
 * @create date 2024-11-25 9:14
 * @Description：操作 Redis工具类
 * 旁路缓存：
 *      思路：先从缓存总获取维度数据，如果获取到了（缓存命中），直接将换次年中的缓存返回，如果在缓存中没有找到要关联的维度，发送请求到HBase中查询维度，
 *          并将查询到的结果放到缓存中缓存起来，方便下次查询使用
 *      选型：
 *          状态：         性能很好，维护性差，只能自己使用
 *          Redis：       性能不错，维护性好，大家都可以用    √
 *      关于Redis的一些设置：
 *                      key：    维度表：主键值
 *                      type：   string
 *                      expire： 失效时间，避免冷数据常驻内存，给内存带来压力  这里失效时间设置为1天   1day
 *                      注意： 如果维度数据发生了变化，需要将缓存的数据清除掉
 */
public class RedisUtil {
    private static JedisPool jedisPool;
    static {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMinIdle(5);
        jedisPoolConfig.setMaxTotal(100);
        jedisPoolConfig.setMaxIdle(5);
        jedisPoolConfig.setBlockWhenExhausted(true);
        jedisPoolConfig.setMaxWaitMillis(2000);
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPool = new JedisPool(jedisPoolConfig, Constant.REDIS_HOST,6379,10000);
    }

    //获取Jedis
    public static Jedis getJedis(){
        System.out.println("~~~获取Jedis客户端~~~");
        Jedis jedis = jedisPool.getResource();
        return jedis;
    }
    //关闭Jedis
    public static void closeJedis(Jedis jedis){
        System.out.println("~~~关闭Jedis客户端~~~");
        if (jedis != null) {
            jedis.close();
        }

    }

    //获取异步操作redis的连接对象
    public static StatefulRedisConnection<String,String> getRedisAsyncConnection(){
        System.out.println("~~~获取异步操作Redis的客户端~~~");
        RedisClient redisClient = RedisClient.create(Constant.REDIS_CLIENT);
        return redisClient.connect();
    }
    //关闭异步操作redis的连接对象
    public static void closeRedisAsyncConnection(StatefulRedisConnection<String,String> asyncRedisConn){
        System.out.println("~~~关闭异步操作Redis的客户端~~~");
        //如果当前的异步的Redis连接是 非空的，而且是打开着的，那么就请求关闭该链接的函数
        if (asyncRedisConn != null && asyncRedisConn.isOpen()) {
            asyncRedisConn.close();
        }
    }
    //以异步的方式从Redis中取数据
    public static JSONObject readDimAsync(StatefulRedisConnection<String,String> asyncRedisConn,String tableName,String id){
        RedisAsyncCommands<String, String> asyncCommands = asyncRedisConn.async();
        String key = getKey(tableName, id);
        try {
            String dimJsonStr = asyncCommands.get(key).get();
            if (StringUtils.isNotEmpty(dimJsonStr)) {
                JSONObject dimJsonObj = JSON.parseObject(dimJsonStr);
                return dimJsonObj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    //以异步的方式向Redis中写入数据
    public static void writeDimAsync(StatefulRedisConnection<String,String> asyncRedisConn,String tableName,String id,JSONObject dimJsonObj){
        //获取连接
        RedisAsyncCommands<String, String> asyncCommands = asyncRedisConn.async();
        //指定失效时间
        String key = getKey(tableName, id);
        asyncCommands.setex(key,24*60*60, dimJsonObj.toJSONString());
    }

    //从Redis中取数据
    public static JSONObject readDim(Jedis jedis,String tableName,String id){
        //拼接key
       String key = getKey(tableName, id);
       //根据key到Redis中获取维度数据
        String dimJsonStr = jedis.get(key);
        if (StringUtils.isNotEmpty(dimJsonStr)) {
            JSONObject dimJsonObj = JSON.parseObject(dimJsonStr);
            return dimJsonObj;
        }
        return null;
    }


    public static String getKey(String tableName, String id) {
        String key = tableName + ":" + id;
        return key;
    }
    //向Redis中写入数据
    public static void writeDim(Jedis jedis,String tableName,String id,JSONObject dimJsonObj){
        String key = getKey(tableName, id);
        jedis.setex(key,24*60*60,dimJsonObj.toJSONString());
        //System.out.println("~~~向Redis中写入了"+tableName+"中的"+key+"数据~~~");
    }

    public static void main(String[] args) {
        Jedis jedis = getJedis();
        String pong = jedis.ping();
        System.out.println(pong);
        closeJedis(jedis);
    }
}
