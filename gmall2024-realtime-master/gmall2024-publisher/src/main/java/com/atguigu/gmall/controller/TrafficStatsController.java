package com.atguigu.gmall.controller;

import com.atguigu.gmall.bean.TrafficUvCt;
import com.atguigu.gmall.service.TradeStatsService;
import com.atguigu.gmall.service.TrafficStatsService;
import com.atguigu.gmall.util.DateFormatUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

/**
 * @author shichunjian
 * @create date 2024-11-29 12:35
 * @Description： 流量域统计controller
 */

@RestController
class TrafficStatsController {
    //@Autowired 这个注解不加的话，就不会赋值，报空指针异常
    @Autowired
    private TrafficStatsService trafficStatsService;
    @RequestMapping("/ch")
    public String getChUvCt(
            @RequestParam(value = "date",defaultValue = "0") Integer date,
            @RequestParam(value = "limit",defaultValue = "10") Integer limit){
        if (date == 0) {
            date = DateFormatUtil.now();
        }
        List<TrafficUvCt> trafficUvCtList = trafficStatsService.getChUvCt(date, limit);
        //{"status": 0,"data": {"categories": ["苹果","三星","华为","oppo","vivo","小米9"],"series": [{"name": "渠道","data": [5599,6633,5507,5458,9074,8838]}]}}
        //这里采用的是List集合获取元素，然后直接循环遍历拼接
        List chList = new ArrayList();
        List uvList = new ArrayList();
        for (TrafficUvCt trafficUvCt : trafficUvCtList) {
            chList.add(trafficUvCt.getCh());
            uvList.add(trafficUvCt.getUvCt());
        }
        //元素之间用  ","  拼接元素
        //StringUtils.join(chList,"\",\"");
        //StringUtils.join(uvList,",");  这里的是元素之间用这些分隔符分隔，所以，最后一个元素是没有 逗号的
        //直接掐头去尾，替换中间的那部分即可
        //String json = "{\"status\": 0,\"data\": {\"categories\": [\"苹果\",\"三星\",\"华为\",\"oppo\",\"vivo\",\"小米9\"],\"series\": [{\"name\": \"渠道\",\"data\": [5599,6633,5507,5458,9074,8838]}]}}";
        String json = "{\"status\": 0,\"data\": {\"categories\": [\""+StringUtils.join(chList,"\",\"")+"\"],\"series\": [{\"name\": \"渠道\",\"data\": ["+StringUtils.join(uvList,",")+"]}]}}";
        return json;
    }
}
