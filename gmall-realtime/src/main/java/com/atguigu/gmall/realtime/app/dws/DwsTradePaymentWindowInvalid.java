package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TradeOrderBean;
import com.atguigu.gmall.realtime.bean.TradePaymentWindowBean;
import com.atguigu.gmall.realtime.util.ClickHouseUtil;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.cep.time.TimeContext;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Objects;

/**
 * description:
 * Created by ?????? on 2022/3/31
 */
@Deprecated
public class DwsTradePaymentWindowInvalid {
    public static void main(String[] args) throws Exception {
        // TODO 1. ????????????
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // TODO 2. ??????????????????
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(
//                3, Time.days(1), Time.minutes(1)
//        ));
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(
//                "hdfs://hadoop102:8020/ck"
//        );
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3. ??? Kafka dwd_trade_pay_detail_suc ???????????????????????????????????????????????????
        String topic = "dwd_trade_pay_detail_suc";
        String groupId = "dws_trade_payment_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 4. ?????? null ???????????????????????????
        SingleOutputStreamOperator<String> filteredStream = source.filter(
                new FilterFunction<String>() {
                    @Override
                    public boolean filter(String jsonStr) throws Exception {
                        if (jsonStr != null) {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            String provinceId = jsonObj.getString("province_id");
                            String paymentTypeCode = jsonObj.getString("payment_type_code");
                            String paymentTypeName = jsonObj.getString("payment_type_name");
                            String sourceTypeName = jsonObj.getString("source_type_name");
                            Long ts = jsonObj.getLong("ts");
                            if (provinceId != null && paymentTypeCode != null && ts != null
                                    && paymentTypeName != null && sourceTypeName != null) {
                                return true;
                            }
                        }
                        return false;
                    }
                }
        );
        SingleOutputStreamOperator<JSONObject> mappedStream = filteredStream.map(JSON::parseObject);

        // TODO 5. ???????????????
        SingleOutputStreamOperator<JSONObject> withWatermarkStream = mappedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
//                                        System.out.println("watermark = " + jsonObj.getLong("ts") * 1000);
                                        return jsonObj.getLong("ts") * 1000;
                                    }
                                }
                        )
        );

        // TODO 6. ??????????????? order_detail_id ??????
        KeyedStream<JSONObject, String> keyedStream = withWatermarkStream.keyBy(r -> r.getString("id"));

        // TODO 7. ??????
        SingleOutputStreamOperator<JSONObject> processedStream = keyedStream.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<JSONObject> lastDataState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastDataState = getRuntimeContext().getState(
                                new ValueStateDescriptor<JSONObject>("last_data_state", JSONObject.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject lastData = lastDataState.value();
                        if (lastData == null) {
                            ctx.timerService().registerEventTimeTimer(5000L);
                        }
                        lastDataState.update(jsonObj);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws IOException {
                        JSONObject value = lastDataState.value();
                        if (value != null) {
                            out.collect(value);
                        }
                        long currentWatermark = ctx.timerService().currentWatermark();
//                        System.out.println("???????????????" + currentWatermark);
                        lastDataState.clear();
                    }
                }
        );
//        processedStream.print("processedStream $$$");

        // TODO 8. ?????????????????????
        // ???????????? keyedProcessFunction ????????????????????????????????????????????????????????????????????????????????????????????????????????????
        SingleOutputStreamOperator<JSONObject> withWatermarkSecondStream = processedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts") * 1000;
                                    }
                                }
                        )
        );

        // TODO 8. ???????????? id ??????
        KeyedStream<JSONObject, String> keyedByUserIdStream = withWatermarkSecondStream.keyBy(r -> r.getString("user_id"));
//        keyedByUserIdStream.print("keyedByUserIdStream *** ");

        // TODO 9. ?????????????????????????????????????????????
        SingleOutputStreamOperator<TradePaymentWindowBean> paymentWindowBeanStream = keyedByUserIdStream.process(
                new KeyedProcessFunction<String, JSONObject, TradePaymentWindowBean>() {

                    private ValueState<String> lastPaySucDtState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastPaySucDtState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("last_pay_suc_dt_state", String.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<TradePaymentWindowBean> out) throws Exception {
                        String lastPaySucDt = lastPaySucDtState.value();
                        Long ts = jsonObj.getLong("ts");
                        String paySucDt = DateFormatUtil.toDate(ts);

                        Long paymentSucUniqueUserCount = 0L;
                        Long paymentSucNewUserCount = 0L;

                        if (lastPaySucDt == null) {
                            paymentSucUniqueUserCount = 1L;
                            paymentSucNewUserCount = 1L;
                        } else {
                            if (!lastPaySucDt.equals(paySucDt)) {
                                paymentSucUniqueUserCount = 1L;
                            }
                        }
                        lastPaySucDtState.update(paySucDt);

                        TradePaymentWindowBean tradePaymentWindowBean = new TradePaymentWindowBean(
                                "",
                                "",
                                paymentSucUniqueUserCount,
                                paymentSucNewUserCount,
                                ts
                        );

                        long currentWatermark = ctx.timerService().currentWatermark();
//                        System.out.println("currentWatermark = " + currentWatermark);
                        out.collect(tradePaymentWindowBean);
                    }
                }
        );

//        paymentWindowBeanStream.print("paymentWithBeanStream &&& ");

        // TODO 10. ??????
        AllWindowedStream<TradePaymentWindowBean, TimeWindow> windowDS = paymentWindowBeanStream.windowAll(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));

        // TODO 11. ??????
        SingleOutputStreamOperator<TradePaymentWindowBean> aggregatedDS = windowDS
//                .reduce(
//                        new ReduceFunction<TradePaymentWindowBean>() {
//                            @Override
//                            public TradePaymentWindowBean reduce(TradePaymentWindowBean value1, TradePaymentWindowBean value2) throws Exception {
//                                value1.setPaymentSucUniqueUserCount(
//                                        value1.getPaymentSucUniqueUserCount() + value2.getPaymentSucUniqueUserCount()
//                                );
//                                value1.setPaymentSucNewUserCount(
//                                        value1.getPaymentSucNewUserCount() + value2.getPaymentSucNewUserCount()
//                                );
//                                return value1;
//                            }
//                        },
//                        new ProcessAllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>() {
//                            @Override
//                            public void process(Context context, Iterable<TradePaymentWindowBean> elements, Collector<TradePaymentWindowBean> out) throws Exception {
//
//                                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
//                                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
//                                for (TradePaymentWindowBean element : elements) {
//                                    element.setStt(stt);
//                                    element.setEdt(edt);
//                                    element.setTs(System.currentTimeMillis());
//                                    out.collect(element);
//                                }
//                            }
//                        }
//                );
                .aggregate(
                new AggregateFunction<TradePaymentWindowBean, TradePaymentWindowBean, TradePaymentWindowBean>() {
                    @Override
                    public TradePaymentWindowBean createAccumulator() {
                        return new TradePaymentWindowBean(
                                "",
                                "",
                                0L,
                                0L,
                                0L
                        );
                    }

                    @Override
                    public TradePaymentWindowBean add(TradePaymentWindowBean value, TradePaymentWindowBean accumulator) {
//                        System.out.println("value = " + value);
//                        System.out.println("accumulator = " + accumulator);
                        accumulator.setPaymentSucUniqueUserCount(
                                accumulator.getPaymentSucUniqueUserCount() + value.getPaymentSucUniqueUserCount()
                        );
                        accumulator.setPaymentSucNewUserCount(
                                accumulator.getPaymentSucNewUserCount() + value.getPaymentSucNewUserCount()
                        );
                        return accumulator;
                    }

                    @Override
                    public TradePaymentWindowBean getResult(TradePaymentWindowBean accumulator) {
                        return accumulator;
                    }

                    @Override
                    public TradePaymentWindowBean merge(TradePaymentWindowBean a, TradePaymentWindowBean b) {
                        return null;
                    }
                },

                new ProcessAllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<TradePaymentWindowBean> elements, Collector<TradePaymentWindowBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        System.out.println("stt = " + context.window().getStart() + "\nstt = " + stt);
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        System.out.println("edt = " + context.window().getEnd() + "\nedt = " + edt);
                        for (TradePaymentWindowBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        );
        aggregatedDS.print("aggregatedDS >>> ");

        // TODO 12. ????????? OLAP ?????????
        SinkFunction<TradePaymentWindowBean> jdbcSink = ClickHouseUtil.<TradePaymentWindowBean>getJdbcSink(
                "insert into dws_trade_payment_suc_window values(?,?,?,?,?)"
        );
        aggregatedDS.addSink(jdbcSink);

        env.execute();
    }
}
