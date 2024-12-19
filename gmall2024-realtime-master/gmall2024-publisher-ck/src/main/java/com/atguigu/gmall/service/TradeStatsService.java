package com.atguigu.gmall.service;

import com.atguigu.gmall.bean.TradeProvinceOrderAmount;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author shichunjian
 * @create date 2024-11-28 17:13
 * @Description： 交易域统计service接口
 */
public interface TradeStatsService {
    //这里把从controller获取的日期，作为变量传给mapper,然后mapper补全date的SQL就可以查库了，从而获取查询结果 获取某天总交易额
    BigDecimal getGMV(Integer date);
    //这里把从controller获取的日期，作为变量传给mapper,然后mapper补全date的SQL就可以查库了，从而获取查询结果 获取某天各个省的订单金额

    List<TradeProvinceOrderAmount> getProvinceAmount(Integer date);
}
