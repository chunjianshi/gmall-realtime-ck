package com.atguigu.gmall.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * @author shichunjian
 * @create date 2024-11-24 19:37
 * @Description：
 */
@Data
@AllArgsConstructor
@Builder
public class TradeOrderBean {
    // 窗口起始时间
    String stt;
    // 窗口关闭时间
    String edt;
    // 当天日期
    String curDate;
    // 下单独立用户数
    Long orderUniqueUserCount;
    // 下单新用户数
    Long orderNewUserCount;
    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}
