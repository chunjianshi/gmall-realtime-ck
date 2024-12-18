package com.atguigu.gmall.service;

import com.atguigu.gmall.bean.TrafficUvCt;

import java.util.List;

/**
 * @author shichunjian
 * @create date 2024-11-29 11:33
 * @Description： 流量域统计Service接口
 */

public interface TrafficStatsService {
    //获取某天各个渠道独立访客数
    List<TrafficUvCt> getChUvCt(Integer date,Integer limit);
}
