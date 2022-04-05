package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.PhoenixUtil;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * description:
 * Created by 铁盾 on 2022/3/23
 */
public class MyPhoenixSink implements SinkFunction<JSONObject> {

    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        String sinkTable = jsonObj.getString("sinkTable");
        System.out.println("sinkTable = " + sinkTable);
        jsonObj.remove("sinkTable");
        PhoenixUtil.insertValues(sinkTable, jsonObj);
    }
}
