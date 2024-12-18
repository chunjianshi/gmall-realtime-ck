package com.atguigu.gmall.realtime.dwd.test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
* @author shichunjian
* @create date 2024-10-29 18:13
* @version
* @Description：
*/ // 员工类
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Emp {
    Integer empno;
    String ename;
    Integer deptno;
    Long ts;
}