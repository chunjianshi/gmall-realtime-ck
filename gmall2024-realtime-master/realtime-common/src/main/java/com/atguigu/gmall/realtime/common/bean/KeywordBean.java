package com.atguigu.gmall.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author shichunjian
 * @create date 2024-12-17 下午1:59
 * @Description： clickhouse转化成流需要
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordBean {
    // 窗口起始时间
    private String stt;

    // 窗口闭合时间
    private String edt;

    // 当天日期
    private String cur_date;

    // 关键词
    private String keyword;

    // 关键词出现频次
    private Long keyword_count;
}
