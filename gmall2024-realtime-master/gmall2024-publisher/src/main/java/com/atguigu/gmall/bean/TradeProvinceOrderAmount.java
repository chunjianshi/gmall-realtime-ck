package com.atguigu.gmall.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author shichunjian
 * @create date 2024-11-28 19:23
 * @Description： 定义省份交易订单金额的实体类
 */
@Data
@AllArgsConstructor
public class TradeProvinceOrderAmount {
    // 省份名称
    String provinceName;
    // 下单金额
    Double orderAmount;
}