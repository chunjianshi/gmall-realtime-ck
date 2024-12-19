package com.atguigu.gmall.service.impl;

import com.atguigu.gmall.bean.TradeProvinceOrderAmount;
import com.atguigu.gmall.mapper.TradeStatsMapper;
import com.atguigu.gmall.service.TradeStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author shichunjian
 * @create date 2024-11-28 17:18
 * @Description： 交易域统计service接口实现类
 */

//这个注解 是告诉这是一个方法，否则无法调用
@Service
public class TradeStatsServiceImpl implements TradeStatsService {
    //这里是获取mapper从数据库里面的查询的结果，然后 把获取到的日期 date 作为变量传给 mapper，补全SQL，然后查询数据
    @Autowired
    private TradeStatsMapper tradeStatsMapper;
    @Override
    public BigDecimal getGMV(Integer date) {
        return tradeStatsMapper.selectGMV(date);
    }

    //从获取mapper从数据库里面的查询的结果，然后 把获取到的日期 date 作为变量传给 mapper，补全SQL 获取某天各个省的订单金额
    @Override
    public List<TradeProvinceOrderAmount> getProvinceAmount(Integer date) {
        return tradeStatsMapper.selectProvinceAmount(date);
    }
}
