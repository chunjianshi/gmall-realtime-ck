package com.atguigu.gmall.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author shichunjian
 * @create date 2024-12-18 下午12:10
 * @Description：
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class withProvinceDSBean {
    private String   stt;
    private String   edt;
    private String   curDate;
    private String   provinceId;
    private String   provinceName;
    private String   orderCount;
    private String   orderAmount;
    private String   ts;
    private String   orderIdSet;
}
