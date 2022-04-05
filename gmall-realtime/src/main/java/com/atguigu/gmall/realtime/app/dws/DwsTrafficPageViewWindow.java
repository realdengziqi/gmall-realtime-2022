package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.gmall.realtime.util.ClickHouseUtil;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
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
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

/**
 * description:
 * Created by 铁盾 on 2022/3/30
 */
public class DwsTrafficPageViewWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // TODO 2. 状态后端设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(
                RestartStrategies.failureRateRestart(
                        3, Time.days(1), Time.minutes(1)
                )
        );
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                "hdfs://hadoop102:8020/ck"
        );
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3. 读取 Kafka dwd_page_log 数据，封装为流
        String topic = "dwd_page_log";
        String groupId = "dws_traffic_page_view_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 4. 转换数据结构 String -> JSONObject
        SingleOutputStreamOperator<JSONObject> mappedStream = source.map(JSON::parseObject);

        // TODO 5. 设置水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = mappedStream.assignTimestampsAndWatermarks(
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
        );

        // TODO 6. 按照 mid 分组
        KeyedStream<JSONObject, String> keyedStream = withWatermarkDS.keyBy(r -> r.getJSONObject("common").getString("mid"));

        // TODO 7. 鉴别独立访客，转换数据结构
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> uvStream = keyedStream.process(
                new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {

                    private ValueState<String> homeLastVisitDt;
                    private ValueState<String> detailLastVisitDt;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        homeLastVisitDt = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("home_last_visit_dt", String.class)
                        );
                        detailLastVisitDt = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("detail_last_visit_dt", String.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        String homeLastDt = homeLastVisitDt.value();
                        String detailLastDt = detailLastVisitDt.value();

                        JSONObject page = jsonObj.getJSONObject("page");
                        String pageId = page.getString("page_id");
                        Long ts = jsonObj.getLong("ts");
                        String visitDt = DateFormatUtil.toDate(ts);

                        Long homeUvCt = 0L;
                        Long detailUvCt = 0L;

                        if (pageId.equals("home")) {
                            if (homeLastDt == null || !homeLastDt.equals(visitDt)) {
                                homeUvCt = 1L;
                                homeLastVisitDt.update(visitDt);
                            }
                        }

                        if (pageId.equals("good_detail")) {
                            if (detailLastDt == null || !detailLastDt.equals(visitDt)) {
                                detailUvCt = 1L;
                                detailLastVisitDt.update(visitDt);
                            }
                        }

                        TrafficHomeDetailPageViewBean trafficHomeDetailPageViewBean = new TrafficHomeDetailPageViewBean(
                                "",
                                "",
                                homeUvCt,
                                detailUvCt,
                                0L
                        );
                        out.collect(trafficHomeDetailPageViewBean);
                    }
                }
        );
        
        // TODO 8. 开窗
        AllWindowedStream<TrafficHomeDetailPageViewBean, TimeWindow> windowStream = uvStream.windowAll(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));
        
        // TODO 9. 聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> aggregateDS = windowStream.aggregate(
                new AggregateFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean createAccumulator() {
                        return new TrafficHomeDetailPageViewBean(
                                "",
                                "",
                                0L,
                                0L,
                                0L
                        );
                    }

                    @Override
                    public TrafficHomeDetailPageViewBean add(TrafficHomeDetailPageViewBean value, TrafficHomeDetailPageViewBean accumulator) {
                        accumulator.setHomeUvCt(accumulator.getHomeUvCt() + value.getHomeUvCt());
                        accumulator.setGoodDetailUvCt(
                                accumulator.getGoodDetailUvCt() + value.getGoodDetailUvCt());
                        return accumulator;
                    }

                    @Override
                    public TrafficHomeDetailPageViewBean getResult(TrafficHomeDetailPageViewBean accumulator) {
                        return accumulator;
                    }

                    @Override
                    public TrafficHomeDetailPageViewBean merge(TrafficHomeDetailPageViewBean a, TrafficHomeDetailPageViewBean b) {
                        return null;
                    }
                },
                new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(window.getStart());
                        String edt = DateFormatUtil.toYmdHms(window.getEnd());

                        for (TrafficHomeDetailPageViewBean value : values) {
                            value.setStt(stt);
                            value.setEdt(edt);
                            value.setTs(System.currentTimeMillis());
                            out.collect(value);
                        }
                    }
                }
        );
        aggregateDS.print("aggregateDS >>>");

        // TODO 10. 写出到 OLAP 数据库
        SinkFunction<TrafficHomeDetailPageViewBean> jdbcSink = ClickHouseUtil.<TrafficHomeDetailPageViewBean>getJdbcSink(
                "insert into dws_traffic_page_view_window values(?,?,?,?,?)"
        );
        aggregateDS.<TrafficHomeDetailPageViewBean>addSink(jdbcSink);

        env.execute();
    }
}
