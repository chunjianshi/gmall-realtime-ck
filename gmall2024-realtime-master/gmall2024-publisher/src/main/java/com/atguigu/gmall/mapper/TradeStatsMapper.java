package com.atguigu.gmall.mapper;

import com.atguigu.gmall.bean.TradeProvinceOrderAmount;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author shichunjian
 * @create date 2024-11-28 17:02
 * @Description： 交易域mapper接口  和数据库进行直接操作的层
 */
public interface TradeStatsMapper {
    //从数据库获取某天总交易额  查库的SQL 增删改查 都可以 我们这里只是查询
    //插入的SQL
    //@Insert("")
    //删除的SQL
    //@Delete("")
    //更新的SQL
    //@Update("")
    //查询的SQL
    //@Select("SELECT SUM(order_amount) FROM dws_trade_province_order_window PARTITION par20241128")
    @Select("SELECT SUM(order_amount) FROM dws_trade_province_order_window PARTITION par#{date}")
    BigDecimal selectGMV(Integer date);

    //从doris数据库获取某天各个省的订单金额
    @Select("SELECT province_name, SUM(order_amount) AS total_amount FROM dws_trade_province_order_window PARTITION par#{date} GROUP BY province_name ORDER BY total_amount")
    List<TradeProvinceOrderAmount> selectProvinceAmount(Integer date);
}
