package com.atguigu.gmall.realtime.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.test.bean.WatermarkTwiceBean;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * description:
 * Created by 铁盾 on 2022/4/1
 */
public class WatermarkTwiceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<String> source = env.addSource(KafkaUtil.getKafkaConsumer("watermark_twice", "wtt"));

        SingleOutputStreamOperator<JSONObject> mappedStream = source.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = mappedStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forMonotonousTimestamps()
                .withTimestampAssigner(
                        new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                System.out.println("jsonObj" + jsonObj);
                                return jsonObj.getLong("ts");
                            }
                        }
                )
        );
        KeyedStream<JSONObject, String> keyedStream = withWatermarkDS.keyBy(r -> r.getString("mid"));
        SingleOutputStreamOperator<JSONObject> firstProcessedStream = keyedStream.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                        String mid = jsonObj.getString("mid");
                        Long ts = jsonObj.getLong("ts");
                        WatermarkTwiceBean watermarkTwiceBean = new WatermarkTwiceBean(
                                mid,
                                ts
                        );
                        long currentWatermark = ctx.timerService().currentWatermark();
                        System.out.println("第一个 KeyedProcessFunction" + currentWatermark);
                        out.collect(jsonObj);
                    }
                }
        )
//                .windowAll(TumblingEventTimeWindows.of(Time.seconds(3L)))
//                .process(
//                        new ProcessAllWindowFunction<WatermarkTwiceBean, WatermarkTwiceBean, TimeWindow>() {
//                            @Override
//                            public void process(Context context, Iterable<WatermarkTwiceBean> elements, Collector<WatermarkTwiceBean> out) throws Exception {
//                                long stt = context.window().getStart();
//                                long edt = context.window().getEnd();
//                                System.out.println("窗口范围 " + stt + " ~ " + edt);
//                                for (WatermarkTwiceBean element : elements) {
//                                    System.out.println(element);
//                                }
//                                System.out.println("窗口范围 " + stt + " ~ " + edt + " 结束");
//                            }
//                        }
//                )
                ;

        KeyedStream<JSONObject, String> keyedStream1 = firstProcessedStream.keyBy(r -> r.getString("mid"));

        keyedStream1.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                        long currentWatermark = ctx.timerService().currentWatermark();
                        System.out.println("第二次分组后的水位线" + currentWatermark);
                    }
                }
        )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(3L)))
                .process(
                        new ProcessAllWindowFunction<JSONObject, JSONObject, TimeWindow>() {
                            @Override
                            public void process(Context context, Iterable<JSONObject> elements, Collector<JSONObject> out) throws Exception {
                                long stt = context.window().getStart();
                                long edt = context.window().getEnd();
                                System.out.println("窗口范围 " + stt + " ~ " + edt);
                                for (JSONObject element : elements) {
                                    System.out.println(element);
                                }
                                System.out.println("窗口范围 " + stt + " ~ " + edt + " 结束");
                            }
                        }
                )
        ;


        env.execute();
    }
}
