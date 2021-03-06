package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TrafficPageViewBean;
import com.atguigu.gmall.realtime.util.ClickHouseUtil;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.cep.*;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * description:
 * Created by ?????? on 2022/3/29
 */
public class DwsTrafficChannelPageViewWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1. ????????????
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // TODO 2. ??????????????????
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

        // TODO 3. ??? kafka dwd_page_log ???????????????????????????????????????
        String topic = "dwd_page_log";
        String groupId = "dws_traffic_channel_page_view_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> pageLogSource = env.addSource(kafkaConsumer);

        // TODO 4. ???????????????????????????
        SingleOutputStreamOperator<JSONObject> jsonObjStream = pageLogSource.map(JSON::parseObject);

        // TODO 5. ??????????????????????????????????????????????????????????????????????????????
        SingleOutputStreamOperator<TrafficPageViewBean> mainStream = jsonObjStream.map(
                new MapFunction<JSONObject, TrafficPageViewBean>() {

                    @Override
                    public TrafficPageViewBean map(JSONObject jsonObj) throws Exception {
                        JSONObject common = jsonObj.getJSONObject("common");
                        JSONObject page = jsonObj.getJSONObject("page");

                        // ?????? ts
                        Long ts = jsonObj.getLong("ts");

                        // ??????????????????
                        String vc = common.getString("vc");
                        String ch = common.getString("ch");
                        String ar = common.getString("ar");
                        String isNew = common.getString("is_new");

                        // ????????????????????????
                        Long duringTime = page.getLong("during_time");

                        // ?????????????????????????????????
                        Long uvCt = 0L;
                        Long svCt = 0L;
                        Long pvCt = 1L;
                        Long ujCt = 0L;

                        // ????????????????????????????????????????????????
                        String lastPageId = page.getString("last_page_id");
                        if (lastPageId == null) {
                            svCt = 1L;
                        }

                        // ??????????????????
                        TrafficPageViewBean trafficPageViewBean = new TrafficPageViewBean(
                                "",
                                "",
                                vc,
                                ch,
                                ar,
                                isNew,
                                uvCt,
                                svCt,
                                pvCt,
                                duringTime,
                                ujCt,
                                ts
                        );
                        return trafficPageViewBean;
                    }
                }
        );

        mainStream.print("mainStream &&&");

        // TODO 6. ??? Kafka ?????????????????????????????????????????????????????????????????????????????????????????????
        // 6.1 ??? Kafka dwd_traffic_user_jump_detail ???????????????????????????????????????
        String ujdTopic = "dwd_traffic_user_jump_detail";
        FlinkKafkaConsumer<String> ujdKafkaConsumer = KafkaUtil.getKafkaConsumer(ujdTopic, groupId);
        DataStreamSource<String> ujdSource = env.addSource(ujdKafkaConsumer);
        SingleOutputStreamOperator<TrafficPageViewBean> ujdMappedStream =
                ujdSource.map(jsonStr -> JSON.parseObject(jsonStr, TrafficPageViewBean.class));

        // 6.2 ??? Kafka dwd_traffic_unique_visitor_detail ?????????????????????????????????????????????
        String uvTopic = "dwd_traffic_unique_visitor_detail";
        FlinkKafkaConsumer<String> uvKafkaConsumer = KafkaUtil.getKafkaConsumer(uvTopic, groupId);
        DataStreamSource<String> uvSource = env.addSource(uvKafkaConsumer);
        SingleOutputStreamOperator<TrafficPageViewBean> uvMappedStream =
                uvSource.map(jsonStr -> JSON.parseObject(jsonStr, TrafficPageViewBean.class));

        // 6.3 ???????????????
        DataStream<TrafficPageViewBean> pageViewBeanDS = mainStream
                .union(ujdMappedStream)
                .union(uvMappedStream);

        pageViewBeanDS.print("pageViewBeanDS %%%");

        // TODO 7. ???????????????
        SingleOutputStreamOperator<TrafficPageViewBean> withWatermarkStream = pageViewBeanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
//                        .<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofMillis(3000L))
                        .<TrafficPageViewBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TrafficPageViewBean>() {
                                    @Override
                                    public long extractTimestamp(TrafficPageViewBean trafficPageViewBean, long recordTimestamp) {
                                        return trafficPageViewBean.getTs();
                                    }
                                }
                        )
        );
//        withWatermarkStream.print("mainStream>>>");

        // TODO 8. ??????????????????
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> keyedBeanStream = withWatermarkStream.keyBy(trafficPageViewBean ->
                Tuple4.of(
                        trafficPageViewBean.getVc(),
                        trafficPageViewBean.getCh(),
                        trafficPageViewBean.getAr(),
                        trafficPageViewBean.getIsNew()
                )
                , Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING)
        );

        // TODO 9. ??????
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowStream = keyedBeanStream.window(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));

        // TODO 10. ????????????
        SingleOutputStreamOperator<TrafficPageViewBean> reducedStream = windowStream.reduce(
                new ReduceFunction<TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                        value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                        value1.setUjCt(value1.getUjCt() + value2.getUjCt());
                        return value1;
                    }
                },
                new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void process(Tuple4<String, String, String, String> key, Context context, Iterable<TrafficPageViewBean> elements, Collector<TrafficPageViewBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        for (TrafficPageViewBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        );

        reducedStream.print("reducedStream***");

        // TODO 11. ?????? OLAP ?????????
        reducedStream.addSink(ClickHouseUtil.<TrafficPageViewBean>getJdbcSink(
                "insert into dws_traffic_channel_page_view_window values(?,?,?,?,?,?,?,?,?,?,?,?)"
        ));

        env.execute();
    }
}
