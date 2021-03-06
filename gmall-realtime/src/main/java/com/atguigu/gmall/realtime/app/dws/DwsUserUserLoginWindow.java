package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.UserLoginBean;
import com.atguigu.gmall.realtime.util.ClickHouseUtil;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.security.User;

/**
 * description:
 * Created by ?????? on 2022/3/30
 */
public class DwsUserUserLoginWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1. ????????????
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // TODO 2. ??????????????????
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
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

        // TODO 3. ??????????????????????????????
        String topic = "dwd_page_log";
        String groupId = "dws_user_user_login_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> pageLogSource = env.addSource(kafkaConsumer);

        // TODO 4. ??????????????????
        SingleOutputStreamOperator<JSONObject> mappedStream = pageLogSource.map(JSON::parseObject);

        // TODO 5. ?????????????????????????????? id ?????? null ?????????????????????
        SingleOutputStreamOperator<JSONObject> filteredStream = mappedStream.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        return jsonObj.getJSONObject("common")
                                .getString("uid") != null
                                && jsonObj
                                .getJSONObject("page")
                                .getString("last_page_id") == null;
                    }
                }
        );

        // TODO 6. ???????????????
        SingleOutputStreamOperator<JSONObject> streamOperator = filteredStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts");
                                    }
                                }
                        )
        );

        // TODO 7. ?????? mid ??????
        KeyedStream<JSONObject, String> keyedStream
                = streamOperator.keyBy(r -> r.getJSONObject("common").getString("mid"));

        // TODO 8. ????????????????????????????????????????????????????????????????????????
        SingleOutputStreamOperator<UserLoginBean> backUniqueUserStream = keyedStream
                .process(
                        new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {

                            private ValueState<String> lastLoginDtState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                lastLoginDtState = getRuntimeContext().getState(
                                        new ValueStateDescriptor<String>("last_lgoin_dt", String.class)
                                );
                            }

                            @Override
                            public void processElement(JSONObject jsonObj, Context ctx, Collector<UserLoginBean> out) throws Exception {
                                String lastLoginDt = lastLoginDtState.value();

                                // ??????????????????????????????????????????????????????
                                long backCt = 0L;
                                long uuCt = 0L;

                                // ????????????????????????
                                Long ts = jsonObj.getLong("ts");
                                String loginDt = DateFormatUtil.toDate(ts);

                                if (lastLoginDt != null) {
                                    // ???????????????????????????????????????
                                    if (!loginDt.equals(lastLoginDt)) {
                                        uuCt = 1L;
                                    }

                                    // ???????????????????????????
                                    // ??????????????????????????????????????????
                                    Long lastLoginTs = DateFormatUtil.toTs(lastLoginDt);
                                    long days = (ts - lastLoginTs) / 1000 / 3600 / 24;

                                    if (days > 7) {
                                        backCt = 1L;
                                    }
                                } else {
                                    uuCt = 1L;
                                }

                                out.collect(new UserLoginBean(
                                        "",
                                        "",
                                        backCt,
                                        uuCt,
                                        ts
                                ));
                            }
                        }
                );

        // TODO 9. ??????
        AllWindowedStream<UserLoginBean, TimeWindow> windowStream = backUniqueUserStream.windowAll(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));

        // TODO 10. ??????
        SingleOutputStreamOperator<UserLoginBean> reducedStream = windowStream.reduce(
                new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                        value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                        value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                        return value1;
                    }
                },
                new ProcessAllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<UserLoginBean> elements, Collector<UserLoginBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        for (UserLoginBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        );
        reducedStream.print("reducedStream >>> ");

        // TODO 11. ?????? OLAP ?????????
        SinkFunction<UserLoginBean> jdbcSink = ClickHouseUtil.<UserLoginBean>getJdbcSink(
                "insert into dws_user_user_login_window values(?,?,?,?,?)"
        );
        reducedStream.addSink(jdbcSink);

        env.execute();
    }
}
