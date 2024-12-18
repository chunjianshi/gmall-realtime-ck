package com.atguigu.gmall.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.util.Set;

/**
 * @author shichunjian
 * @create date 2024-11-27 19:34
 * @Description： 严格定义实体类关联后的json的对象的所有用到的具体的字段信息，这里配置啥字段，输出的json字段的格式就是啥！
 * 就是  kafka中过来的订单明细表 和 HBase过来的省份的表  关联后的结果表的 字段的格式及字段的类型，在这里进行严格的定义！
 */
@Data
@AllArgsConstructor
@Builder
public class TradeProvinceOrderBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 当天日期
    String curDate;
    // 省份 ID
    String provinceId;
    // 省份名称
    @Builder.Default
    String provinceName = "";

    // 累计下单次数
    Long orderCount;
    // 累计下单金额
    BigDecimal orderAmount;

    // 时间戳
    @JSONField(serialize = false)
    Long ts;

    @JSONField(serialize = false)
    Set<String> orderIdSet;
}