package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TrafficPageViewBean;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

/**
 * description:
 * Created by 铁盾 on 2022/3/29
 */
public class DwdTrafficUniqueVisitorDetail {
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

        // TODO 5. 按照 mid 分组
        KeyedStream<JSONObject, String> keyedStream = cleanedStream
                .keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        // TODO 6. 通过 Flink 状态编程过滤独立访客记录
        SingleOutputStreamOperator<JSONObject> filteredStream = keyedStream.filter(
                new RichFilterFunction<JSONObject>() {

                    private ValueState<String> lastVisitDt;

                    @Override
                    public void open(Configuration paramenters) throws Exception {
                        super.open(paramenters);
                        ValueStateDescriptor<String> valueStateDescriptor =
                                new ValueStateDescriptor<>("last_visit_dt", String.class);
                        valueStateDescriptor.enableTimeToLive(
                                StateTtlConfig.newBuilder(Time.days(1L)).build()
                        );
                        lastVisitDt = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        String visitDt = DateFormatUtil.toDate(jsonObj.getLong("ts"));
                        String lastDt = lastVisitDt.value();
                        if (lastDt == null || !lastDt.equals(visitDt)) {
                            lastVisitDt.update(visitDt);
                            return true;
                        }
                        return false;
                    }
                }
        );

        // TODO 7. 转换流中数据结构为实体类，再转换为 JSONString
        SingleOutputStreamOperator<String> uvStream = filteredStream.map(
                new MapFunction<JSONObject, String>() {
                    @Override
                    public String map(JSONObject jsonObj) throws Exception {
                        JSONObject common = jsonObj.getJSONObject("common");
                        Long ts = jsonObj.getLong("ts");

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
                                1L,
                                0L,
                                0L,
                                0L,
                                0L,
                                ts
                        );

                        // 转换为 JSONString
                        String jsonStr = JSON.toJSONString(trafficPageViewBean);
                        return jsonStr;
                    }
                }
        );
        uvStream.print("uvStream >>>");

        // TODO 8. 将独立访客数据写入 Kafka dwd_traffic_unique_visitor_detail 主题
        String targetTopic = "dwd_traffic_unique_visitor_detail";
        FlinkKafkaProducer<String> kafkaProducer = KafkaUtil.getKafkaProducer(targetTopic);
        uvStream.addSink(kafkaProducer);

        env.execute();
    }
}
