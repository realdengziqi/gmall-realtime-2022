package com.atguigu.gmall.realtime.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * description:
 * Created by 铁盾 on 2022/3/30
 */
public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        DataStreamSource<String> source = env.fromElements(
//                "{\n" +
//                        "    \"key\": \"A\",\n" +
//                        "    \"ts\": 1000\n" +
//                        "}",
//                "{\n" +
//                        "    \"key\": \"B\",\n" +
//                        "    \"ts\": 50000\n" +
//                        "}",
//                "{\n" +
//                        "    \"key\": \"C\",\n" +
//                        "    \"ts\": 100000\n" +
//                        "}"
//        );

        DataStreamSource<String> source = env.addSource(KafkaUtil.getKafkaConsumer("test_topic", "test_topic_group"));

        SingleOutputStreamOperator<JSONObject> mappedStream = source.map(JSON::parseObject);

        SingleOutputStreamOperator<JSONObject> withWatermarkStream = mappedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                        System.out.println("element.getLong(\"ts\") = " + element.getLong("ts"));
                                        return element.getLong("ts");
                                    }
                                }
                        )
        );

//        SingleOutputStreamOperator<JSONObject> withWatermarkStream1 = withWatermarkStream.assignTimestampsAndWatermarks(
//                WatermarkStrategy
//                        .<JSONObject>forMonotonousTimestamps()
//                        .withTimestampAssigner(
//                                new SerializableTimestampAssigner<JSONObject>() {
//                                    @Override
//                                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
//                                        System.out.println("element.getLong(\"ts\") - 5000L = " + (element.getLong("ts") - 5000L));
//                                        return element.getLong("ts") - 5000L;
//                                    }
//                                }
//                        )
//        );

        KeyedStream<JSONObject, Integer> keyedStream = withWatermarkStream.keyBy(r -> 1);

        WindowedStream<JSONObject, Integer, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(3L)));
        SingleOutputStreamOperator<String> reducedStream = window.reduce(
                new ReduceFunction<JSONObject>() {
                    @Override
                    public JSONObject reduce(JSONObject value1, JSONObject value2) throws Exception {
                        return value1;
                    }
                },
                new ProcessWindowFunction<JSONObject, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, Context context, Iterable<JSONObject> elements, Collector<String> out) throws Exception {

                        long currentWatermark = context.currentWatermark();
                        for (JSONObject element : elements) {
                            System.out.println("element = " + element);
                            System.out.println("currentWatermark = " + currentWatermark);
                            out.collect(element.toJSONString());
                        }
                    }
                }
        );

        reducedStream.print("reducedStream >>> ");

        env.execute();
    }
}
