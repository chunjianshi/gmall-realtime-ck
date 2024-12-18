package com.atguigu.gmall.util;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;

/**
 * @author shichunjian
 * @create date 2024-11-28 17:41
 * @Description： 将当前日期转成制定格式日期返回的方法
 */
public class DateFormatUtil {
    //将当前日期转成制定格式日期返回
    public static Integer now(){
        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return Integer.valueOf(yyyyMMdd);
    }
}
