package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TrafficPageViewBean;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * description:
 * Created by 铁盾 on 2022/3/29
 */
public class
DwdTrafficUserJumpDetail {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // TODO 2. 状态后端设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, Time.days(1), Time.minutes(1)
        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                "hdfs://hadoop102:8020/ck"
        );
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3. 从 kafka topic_log 主题读取日志数据，封装为流
        String topic = "topic_log";
        String groupId = "dwd_traffic_user_jump_detail";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> pageLog = env.addSource(kafkaConsumer);

         /*DataStream<String> kafkaDS = env
            .fromElements(
                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"home\"},\"ts\":15000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"detail\"},\"ts\":30000} "
            );*/

        // TODO 4. 数据清洗，转换结构
        SingleOutputStreamOperator<JSONObject> cleanedStream = pageLog.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            if(jsonObj.getJSONObject("page") != null) {
                                jsonObj.remove("err");
                                jsonObj.remove("displays");
                                jsonObj.remove("actions");
                                out.collect(jsonObj);
                            }
                        } catch (Exception e) {
                            System.out.println("脏数据~");
                            e.printStackTrace();
                        }
                    }
                }
        );

        // TODO 5. 设置水位线，用于用户跳出统计
        SingleOutputStreamOperator<JSONObject> withWatermarkStream = cleanedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
//                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofMillis(3000L))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts");
                                    }
                                }
                        )
        );

        // TODO 6. 按照 mid 分组
        KeyedStream<JSONObject, String> keyedStream = withWatermarkStream.keyBy(jsonOjb -> jsonOjb.getJSONObject("common").getString("mid"));
//        keyedStream.print("keyedStream --- >>> ");

        // TODO 7. 定义 CEP 匹配规则
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(
                new SimpleCondition<JSONObject>() {

                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        System.out.println("last_page_id >>> " + lastPageId);
                        return lastPageId == null;
                    }
                }
        ).next("second").where(
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        return true;
                    }
                }
                // 上文调用了同名 Time 类，此处需要使用全类名
        ).within(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L));

        // TODO 8. 把 Pattern 应用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        // TODO 9. 提取超时流
        OutputTag<JSONObject> timeoutTag = new OutputTag<JSONObject>("timeoutTag") {
        };

        SingleOutputStreamOperator<JSONObject> flatSelectStream = patternStream.flatSelect(
                timeoutTag,
                new PatternFlatTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp, Collector<JSONObject> out) throws Exception {
                        List<JSONObject> elements = pattern.get("first");
                        for (JSONObject element : elements) {
                            out.collect(element);
                        }
                    }
                },
                new PatternFlatSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<JSONObject> out) throws Exception {
                    }
                }
        );

        DataStream<JSONObject> uJDStream = flatSelectStream.getSideOutput(timeoutTag);
        uJDStream.print("uJDStream *** >>> ");

        // TODO 10. 将跳出流数据结构由 JSONObject 转换为实体类，再转换为 JSONString
        SingleOutputStreamOperator<String> ujdMappedStream = uJDStream.map(
                jsonObj -> {
                    JSONObject common = jsonObj.getJSONObject("common");
                    Long ts = jsonObj.getLong("ts") + 10 * 1000L;

                    // 获取维度信息
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String isNew = common.getString("is_new");

                    // 封装为实体类
                    TrafficPageViewBean trafficPageViewBean = new TrafficPageViewBean(
                            "",
                            "",
                            vc,
                            ch,
                            ar,
                            isNew,
                            0L,
                            0L,
                            0L,
                            0L,
                            1L,
                            ts
                    );
                    return JSON.toJSONString(trafficPageViewBean);
                }
        );
        ujdMappedStream.print("ujdMappedStream $$$");

        // TODO 11. 将数据写出到 Kafka
        String targetTopic = "dwd_traffic_user_jump_detail";
        FlinkKafkaProducer<String> kafkaProducer = KafkaUtil.getKafkaProducer(targetTopic);
        ujdMappedStream.addSink(kafkaProducer);

        env.execute();
    }
}
