package com.atguigu.gmall.controller;

import com.atguigu.gmall.bean.TradeProvinceOrderAmount;
import com.atguigu.gmall.service.TradeStatsService;
import com.atguigu.gmall.util.DateFormatUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.List;


/**
 * @author shichunjian
 * @create date 2024-11-28 17:29
 * @Description： 交易域统计 Controller
 */

//这个注解 首先声明自己 然后功能还有如果是字符串的话，会直接返回
@RestController
public class TradeStatsController {
    //定义向下传递的目标
    @Autowired
    private TradeStatsService tradeStatsService;

    //从请求的地址中 http://localhost:8070/gmv?data=20241128 拦截gmv 及获取 date的日期
    @RequestMapping("/gmv")
    //获取date日期的方法，需要自定义 这里还有如果没有传参的话，给默认的date赋予当前日期 即获取不到date，那么值就是默认0 ，如果为0就开始给date传当前日期
    public String getGMV(@RequestParam(value = "date",defaultValue = "0") Integer date){
        if (date == 0) {
            //说明请求的时候，没有传递日期参数，将当天的日期作为查询的日期(这里的函数是自定义的)
            date = DateFormatUtil.now();
        }
        //执行向下传递的操作，把date传递给tradeStatsService,经过查询会把查库等返回的结果返回给 gmv
        BigDecimal gmv = tradeStatsService.getGMV(date);
        //我们得到的返回结果是查询mysql的结果，但是这个结果需要封装成大屏指定格式的json才能识别
        //SELECT SUM(order_amount) FROM dws_trade_province_order_window PARTITION par20241128
        //879246.50
        //{
        //		  "status": 0,
        //		  "data": 1201005.0653228906
        //		}
        //开始封装json 直接把json数据复制过来，然后用变量值替换原来的数据即可
        String json = "{\n" +
                "\t\t  \"status\": 0,\n" +
                "\t\t  \"data\": "+gmv+"\n" +
                "\t\t}";
        return json;
    }

    //
    @RequestMapping("/province")
    public String getProvinceAmount(@RequestParam(value = "date",defaultValue = "0") Integer date){
        if (date == 0) {
            //说明请求的时候，没有传递日期参数，将当天的日期作为查询的日期(这里的函数是自定义的)
            date = DateFormatUtil.now();
        }
        //执行向下传递的操作，把date传递给tradeStatsService,经过查询会把查库等返回的结果返回给 gmv
        List<TradeProvinceOrderAmount> provinceOrderAmountList = tradeStatsService.getProvinceAmount(date);
        //我们得到的返回结果是查询mysql的结果，但是这个结果需要封装成大屏指定格式的json才能识别
        //SELECT province_name, SUM(order_amount) AS total_amount FROM dws_trade_province_order_window PARTITION par20241128  GROUP BY province_name ORDER BY total_amount;
        //河南	69.00
        //青海	399.00
        //目标json
        //{"status": 0,"data": {"mapData": [
        //		      {"name": "北京","value": 6427},
        //		      {"name": "天津","value": 5758}
        //		    ],"valueName": "各省份订单金额"}}
        //开始封装json 由于这里的json结构比较复杂，里面的是未知多的list集合，这就需要拼接SQL，把头和尾拼接，中间遍历拼接即可，遍历的过程用变量替换即可，然后拼接的还有,号
        //定义拼接头
        StringBuilder jsonB = new StringBuilder("{\"status\": 0,\"data\": {\"mapData\": [");
        //拼接中间的部分，中间的是未知数量，所以用循环遍历来实现 先获取数据的长度
        for (int i = 0; i < provinceOrderAmountList.size(); i++) {
            //遍历集合的每一个元素 的省份名称和金额大小
            TradeProvinceOrderAmount provinceOrderAmount = provinceOrderAmountList.get(i);
            //把获取到的省份名称和金额大小的值，拼接到SQL的变量部分
            //jsonB.append("{\"name\": \"北京\",\"value\": 6427}");
            jsonB.append("{\"name\": \""+provinceOrderAmount.getProvinceName()+"\",\"value\": "+provinceOrderAmount.getOrderAmount()+"}");
            //拼接逗号 , 除了最后一行以外，其他的都有
            if (i < provinceOrderAmountList.size() -1) {
                jsonB.append(",");
            }
        }
        //接着把最后的拼接尾巴结合 拼接，这样就获取到了完整的拼接SQL了
        jsonB.append("],\"valueName\": \"各省份订单金额\"}}");
        return jsonB.toString();
    }
}
