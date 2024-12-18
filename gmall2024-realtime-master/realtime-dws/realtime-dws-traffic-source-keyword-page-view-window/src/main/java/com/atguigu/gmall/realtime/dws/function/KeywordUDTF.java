package com.atguigu.gmall.realtime.dws.function;

import com.atguigu.gmall.realtime.dws.util.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @author shichunjian
 * @create date 2024-11-17 19:59
 * @Description：自定义UDTF函数 这里的意义就是把一个字符串的输入，转换成分词后的多个输出，每个输出还得添加 时间（字符串的原来的那个时间）
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String text) {
        if (text == null || text.trim().isEmpty()) {
            return; // 输入为空，直接返回
        }
        try {
            for (String keyword : KeywordUtil.analyze(text)) {
                collect(Row.of(keyword)); // 输出分词结果
            }
        } catch (Exception e) {
            // 打印异常日志或者计入指标系统
            System.err.println("Error during keyword analysis: " + e.getMessage());
        }
    }
}

