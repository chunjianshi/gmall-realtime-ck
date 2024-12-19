package com.atguigu.gmall;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author shichunjian
 * @create date 2024-12-19 上午9:28
 * @Description：
 */

@SpringBootApplication
//实现直接查库的接口的方法的注解，只有这里注册了，主方法才能调用到
@MapperScan(basePackages = "com.atguigu.gmall.mapper")
public class Gmall2024PublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(Gmall2024PublisherApplication.class, args);
    }
}