package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

/**
 * description:
 * Created by 铁盾 on 2022/4/2
 */
public interface DimJoinFunction<T> {
    void join(T obj, JSONObject dimJsonObj) throws Exception;

    //获取维度主键的方法
    String getKey(T obj);
}