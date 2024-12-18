package com.atguigu.gmall.realtime.common.bean;

import com.alibaba.fastjson.JSONObject;

/**
 * @author shichunjian
 * @create date 2024-11-26 17:03
 * @Description：旁路缓存 维度关联 抽出来模板封装的接口
 */
public interface DimJoinFunction<T> {
    void addDims(T obj, JSONObject dimJsonObj);

    String getTableName() ;

    String getRowKey(T obj);

}
