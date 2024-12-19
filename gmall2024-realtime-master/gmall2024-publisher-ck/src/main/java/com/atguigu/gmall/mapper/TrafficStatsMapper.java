package com.atguigu.gmall.mapper;

import com.atguigu.gmall.bean.TrafficUvCt;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @author shichunjian
 * @create date 2024-11-29 11:14
 * @Description： 流量域统计Mapper接口
 */
public interface TrafficStatsMapper {
    //获取某天各个渠道独立访客
        //sql查询语句：SELECT ch,SUM(uv_ct) AS uv_ct FROM `dws_traffic_vc_ch_ar_is_new_page_view_window` PARTITION par20241123
        // GROUP BY ch ORDER BY uv_ct DESC limit 5;
    //@Select("SELECT ch,SUM(uv_ct) AS uv_ct FROM `dws_traffic_vc_ch_ar_is_new_page_view_window` PARTITION par20241123 GROUP BY ch ORDER BY uv_ct DESC limit 5")
    //一个参数的时候，可以直接使用 #{} 来接收变量，但是2个及以上的时候，就不能这么干了！
    //错误写法@Select("SELECT ch,SUM(uv_ct) AS uv_ct FROM `dws_traffic_vc_ch_ar_is_new_page_view_window` PARTITION par#{date} GROUP BY ch ORDER BY uv_ct DESC limit #{limit}")
    //多个的参数的时候，这里定义的是 param1  param2 param3 ...... 来对应集合里面的值，所以应该这样写
    //@Select("SELECT ch,SUM(uv_ct) AS uv_ct FROM `dws_traffic_vc_ch_ar_is_new_page_view_window` PARTITION par#{param1} GROUP BY ch ORDER BY uv_ct DESC limit #{param2}")
    //上面写法是对的，但是不够直观和统一，不好识别，为了统一规范，我们可以在传参的时候，给变量重新定义别名
    @Select("SELECT ch,SUM(uv_ct) AS uv_ct FROM `dws_traffic_vc_ch_ar_is_new_page_view_window`  GROUP BY ch ORDER BY uv_ct DESC limit #{limit}")
        //SQL查询结果
        //Appstore		68
        //xiaomi		44
        //oppo			28
        //wandoujia		10
        //web			10
    //定义List集合
    //List<TrafficUvCt> selectChUvCt(Integer date,Integer limit);
    //重新定义别名的变量集合 @Param("date") @Param("limit")
    List<TrafficUvCt> selectChUvCt(@Param("date") Integer date,@Param("limit") Integer limit);
}
