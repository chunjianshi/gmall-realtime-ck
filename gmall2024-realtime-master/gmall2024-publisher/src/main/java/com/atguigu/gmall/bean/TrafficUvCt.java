package com.atguigu.gmall.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author shichunjian
 * @create date 2024-11-29 11:19
 * @Description： 渠道独立访客数 实体类
 */
@Data
@AllArgsConstructor
public class TrafficUvCt {
    // 渠道
    String ch;
    // 独立访客数
    Integer uvCt;
}
