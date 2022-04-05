package com.atguigu.gmall.realtime.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.codehaus.jettison.json.JSONString;

import java.util.List;
import java.util.Map;

/**
 * description:
 * Created by 铁盾 on 2022/3/30
 */
public class UjdWatermarkTest {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2. 从 Kafka 读取测试数据
        DataStreamSource<String> source = env.addSource(KafkaUtil.getKafkaConsumer("test_topic", "test_topic_group"));

        // TODO 3. 转换数据结构，设置水位线并按照 mid 分组
        KeyedStream<JSONObject, String> stream = source
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<JSONObject>() {
                                            @Override
                                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                                return element.getLong("ts");
                                            }
                                        }
                                )
                )
                .keyBy(r -> r.getString("mid"));

        // TODO 4. 定义匹配规则
        Pattern<JSONObject, JSONObject> pattern = Pattern
                .<JSONObject>begin("first")
                .where(
                        new SimpleCondition<JSONObject>() {
                            @Override
                            public boolean filter(JSONObject value) throws Exception {
                                return value.getString("last_page_id") == null;
                            }
                        }
                )
                .next("second")
                .where(
                        new SimpleCondition<JSONObject>() {
                            @Override
                            public boolean filter(JSONObject value) throws Exception {
                                return true;
                            }
                        }
                )
                .within(Time.milliseconds(3000L));

        // TODO 5. 把 CEP 匹配规则应用到流上
        PatternStream<JSONObject> cepStream = CEP.pattern(stream, pattern);

        // TODO 6. 提取超时流
        OutputTag<JSONObject> timeoutTag = new OutputTag<JSONObject>("time_out_tag") {
        };
        SingleOutputStreamOperator<JSONObject> cepLastStream = cepStream.flatSelect(
                timeoutTag,
                new PatternFlatTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp, Collector<JSONObject> out) throws Exception {
                        List<JSONObject> ceps = pattern.get("first");
                        for (JSONObject cep : ceps) {
                            out.collect(cep);
                        }
                    }
                },
                new PatternFlatSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<JSONObject> out) throws Exception {

                    }
                }
        );

        DataStream<JSONObject> timeoutStream = cepLastStream
                .getSideOutput(timeoutTag);


//        timeoutStream
                // 为超时流单独设置水位线
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy
//                                .<JSONObject>forMonotonousTimestamps()
//                                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
//                                    @Override
//                                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
//                                        return element.getLong("ts");
//                                    }
//                                })
//                )
//                .process(
//                        new ProcessFunction<JSONObject, JSONObject>() {
//                            @Override
//                            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
//                                System.out.println("侧输出流水位线" + ctx.timerService().currentWatermark());
//                                System.out.println("value = " + value);
//                                out.collect(value);
//                            }
//                        }
//                )
        // TODO 7. 提取出的超时流丢失了分组信息，按照 mid 分组，开窗聚合
        timeoutStream
                .keyBy(r -> r.getString("mid"))
                .process(
                        new KeyedProcessFunction<String, JSONObject, Object>() {
                            @Override
                            public void processElement(JSONObject jsonObj, Context ctx, Collector<Object> out) throws Exception {
                                long currentWatermark = ctx.timerService().currentWatermark();
                                System.out.println("超时流的水位线" + currentWatermark);
                            }
                        }
                )
//                .window(TumblingEventTimeWindows.of(Time.seconds(3L)))
//                .process(new ProcessWindowFunction<JSONObject, String, String, TimeWindow>() {
//                    @Override
//                    public void process(String s, Context context, Iterable<JSONObject> elements, Collector<String> out) throws Exception {
//                        for (JSONObject e : elements) {
//                            out.collect(e.toJSONString());
//                        }
//                    }
//                })
//                .print("reducedDS >>>")
                ;

        env.execute();
    }
}
