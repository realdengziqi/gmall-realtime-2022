package com.atguigu.gmall.realtime.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * description:
 * Created by 铁盾 on 2022/3/15
 */
public class JSONTest {
    public static void main(String[] args) {
        String jsonTest = "{\n" +
                "\t\"hello\": \"string\"\n" +
                "}";
        JSONObject jsonOjb = JSON.parseObject(jsonTest);
        Object hehe = jsonOjb.remove("hello");
        System.out.println("hehe = " + hehe);
        Object hhhhhhhhhhhhhh = jsonOjb.get("hhhhhhhhhhhhhh");
        System.out.println("hhhhhhhhhhhhhh = " + hhhhhhhhhhhhhh);
        jsonOjb.put("hello", hehe);
        String hello = jsonOjb.getString("hello");
        System.out.println("hello = " + hello);
    }


}
