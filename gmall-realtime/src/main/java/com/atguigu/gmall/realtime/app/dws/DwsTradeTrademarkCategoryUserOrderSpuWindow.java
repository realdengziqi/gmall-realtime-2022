package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.bean.TradeTrademarkCategoryUserSpuOrderBean;
import com.atguigu.gmall.realtime.util.ClickHouseUtil;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import com.atguigu.gmall.realtime.util.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * description:
 * Created by 铁盾 on 2022/4/1
 */
public class DwsTradeTrademarkCategoryUserOrderSpuWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(4);

        // TODO 2. 状态后端设置
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );
//        env.setRestartStrategy(
//                RestartStrategies.failureRateRestart(
//                        3, Time.days(1L), Time.minutes(1L)
//                )
//        );
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(
//                "hdfs://hadoop102:8020/ck"
//        );
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3. 从 Kafka dwd_trade_order_detail 主题读取下单明细数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_trademark_category_user_order_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
//        source.print("3 source >>>");

        // TODO 4. 过滤 null 数据并转换数据结构
        SingleOutputStreamOperator<String> filteredDS = source.filter(
                new FilterFunction<String>() {
                    @Override
                    public boolean filter(String jsonStr) throws Exception {
                        if (jsonStr != null) {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            String userId = jsonObj.getString("user_id");
                            String sourceTypeName = jsonObj.getString("source_type_name");
                            if (userId != null && sourceTypeName != null) {
                                return true;
                            }
                        }
                        return false;
                    }
                }
        );
        SingleOutputStreamOperator<JSONObject> mappedStream = filteredDS.map(JSON::parseObject);
//        filteredDS.print("4 filteredDS >>> ");

        // TODO 5. 按照 order_detail_id 分组
        KeyedStream<JSONObject, String> keyedStream = mappedStream.keyBy(r -> r.getString("id"));
//        mappedStream.print("5 mappedStream >>> ");

        // TODO 6. 去重
        SingleOutputStreamOperator<JSONObject> processedStream = keyedStream.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> lastValueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastValueState = getRuntimeContext().getState(
                                new ValueStateDescriptor<JSONObject>("last_value_state", JSONObject.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject lastValue = lastValueState.value();
                        if (lastValue == null) {
                            ctx.timerService().registerProcessingTimeTimer(5000L);
                            lastValueState.update(jsonObj);
                        } else {
                            String lastRowOpTs = lastValue.getString("row_op_ts");
                            String rowOpTs = jsonObj.getString("row_op_ts");
                            if (TimestampLtz3CompareUtil.compare(lastRowOpTs, rowOpTs) <= 0) {
                                lastValueState.update(jsonObj);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws IOException {
                        JSONObject lastValue = this.lastValueState.value();
                        if (lastValue != null) {
                            out.collect(lastValue);
                        }
                        lastValueState.clear();
                    }
                }
        );
//        processedStream.print("6 processedStream >>> ");

        // TODO 7. 转换数据结构
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> javaBeanStream = processedStream.map(
                jsonObj -> {
                    String orderId = jsonObj.getString("order_id");
                    String userId = jsonObj.getString("user_id");
                    String skuId = jsonObj.getString("sku_id");
                    Double splitTotalAmount = jsonObj.getDouble("split_total_amount");
                    Long ts = jsonObj.getLong("ts");
                    TradeTrademarkCategoryUserSpuOrderBean trademarkCategoryUserOrderBean = TradeTrademarkCategoryUserSpuOrderBean.builder()
                            .orderIdSet(new HashSet<String>(
                                    Collections.singleton(orderId)
                            ))
                            .userId(userId)
                            .skuId(skuId)
                            .orderAmount(splitTotalAmount)
                            .ts(ts)
                            .build();
                    return trademarkCategoryUserOrderBean;
                }
        );
//        javaBeanStream.print("7 javaBeanStream >>>");

        // TODO 8. 维度关联
        // 8.1 关联 sku_info 表
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withSkuInfoStream = AsyncDataStream.unorderedWait(
                javaBeanStream,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("dim_sku_info".toUpperCase()) {
                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean javaBean, JSONObject jsonObj) throws Exception {
                        javaBean.setTrademarkId(jsonObj.getString("tm_id".toUpperCase()));
                        javaBean.setCategory3Id(jsonObj.getString("category3_id".toUpperCase()));
                        javaBean.setSpuId(jsonObj.getString("spu_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean javaBean) {
                        return javaBean.getSkuId();
                    }
                },
                60 * 5, TimeUnit.SECONDS
        );
//        withSkuInfoStream.print("8.1 withSkuInfoStream >>> ");

        // 8.2 关联 spu_info 表
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withSpuInfoStream = AsyncDataStream.unorderedWait(
                withSkuInfoStream,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("dim_spu_info".toUpperCase()) {
                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean javaBean, JSONObject dimJsonObj) throws Exception {
                        javaBean.setSpuName(
                                dimJsonObj.getString("spu_name".toUpperCase())
                        );
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean javaBean) {
                        return javaBean.getSpuId();
                    }
                },
                60 * 5, TimeUnit.SECONDS
        );
//        withSpuInfoStream.print("8.2 withSpuInfoStream >>> ");

        // 8.3 关联品牌表 base_trademark
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withTrademarkStream = AsyncDataStream.unorderedWait(
                withSpuInfoStream,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("dim_base_trademark".toUpperCase()) {
                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean javaBean, JSONObject jsonObj) throws Exception {
                        javaBean.setTrademarkName(jsonObj.getString("tm_name".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean javaBean) {
                        return javaBean.getTrademarkId();
                    }
                },
                5 * 60, TimeUnit.SECONDS
        );
//        withTrademarkStream.print("8.3 withTrademarkStream >>> ");

        // 8.4 关联三级分类表 base_category3
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withCategory3Stream = AsyncDataStream.unorderedWait(
                withTrademarkStream,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("dim_base_category3".toUpperCase()) {
                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean javaBean, JSONObject jsonObj) throws Exception {
                        javaBean.setCategory3Name(jsonObj.getString("name".toUpperCase()));
                        javaBean.setCategory2Id(jsonObj.getString("category2_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean javaBean) {
                        return javaBean.getCategory3Id();
                    }
                },
                5 * 60, TimeUnit.SECONDS
        );
//        withCategory3Stream.print("8.4 withCategory3Stream >>> ");

        // 8.5 关联二级分类表 base_category2
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withCategory2Stream = AsyncDataStream.unorderedWait(
                withCategory3Stream,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("dim_base_category2".toUpperCase()) {
                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean javaBean, JSONObject jsonObj) throws Exception {
                        javaBean.setCategory2Name(jsonObj.getString("name".toUpperCase()));
                        javaBean.setCategory1Id(jsonObj.getString("category1_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean javaBean) {
                        return javaBean.getCategory2Id();
                    }
                },
                5 * 60, TimeUnit.SECONDS
        );
//        withCategory2Stream.print("8.5 withCategory2Stream >>> ");

        // 8.6 关联一级分类表 base_category1
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withCategory1Stream = AsyncDataStream.unorderedWait(
                withCategory2Stream,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("dim_base_category1".toUpperCase()) {
                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean javaBean, JSONObject jsonObj) throws Exception {
                        javaBean.setCategory1Name(jsonObj.getString("name".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean javaBean) {
                        return javaBean.getCategory1Id();
                    }
                },
                5 * 60, TimeUnit.SECONDS
        );
//        withCategory1Stream.print("8.6 withCategory1Stream >>> ");

        // TODO 9. 设置水位线
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withWatermarkDS = withCategory1Stream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TradeTrademarkCategoryUserSpuOrderBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TradeTrademarkCategoryUserSpuOrderBean>() {
                                    @Override
                                    public long extractTimestamp(TradeTrademarkCategoryUserSpuOrderBean javaBean, long recordTimestamp) {
                                        return javaBean.getTs() * 1000;
                                    }
                                }
                        )
        );
//        withWatermarkDS.print("9 withWatermarkDS >>> ");

        // TODO 10. 分组
        KeyedStream<TradeTrademarkCategoryUserSpuOrderBean, String> keyedForAggregateStream = withWatermarkDS.keyBy(
                new KeySelector<TradeTrademarkCategoryUserSpuOrderBean, String>() {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean javaBean) throws Exception {
                        return javaBean.getTrademarkId() +
                                javaBean.getTrademarkName() +
                                javaBean.getCategory1Id() +
                                javaBean.getCategory1Name() +
                                javaBean.getCategory2Id() +
                                javaBean.getCategory2Name() +
                                javaBean.getCategory3Id() +
                                javaBean.getCategory3Name() +
                                javaBean.getUserId() +
                                javaBean.getSpuId() +
                                javaBean.getSpuName();
                    }
                }
        );
//        keyedForAggregateStream.print("10 keyedForAggregateStream >>> ");

        // TODO 11. 开窗
        WindowedStream<TradeTrademarkCategoryUserSpuOrderBean, String, TimeWindow> windowDS = keyedForAggregateStream.window(TumblingEventTimeWindows.of(
                Time.seconds(10L)));

        // TODO 12. 聚合
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> reducedStream = windowDS
                .reduce(
                new ReduceFunction<TradeTrademarkCategoryUserSpuOrderBean>() {
                    @Override
                    public TradeTrademarkCategoryUserSpuOrderBean reduce(TradeTrademarkCategoryUserSpuOrderBean value1, TradeTrademarkCategoryUserSpuOrderBean value2) throws Exception {
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
//                        System.out.println("value1 = " + value1);
                        return value1;
                    }
                },
                new ProcessWindowFunction<TradeTrademarkCategoryUserSpuOrderBean, TradeTrademarkCategoryUserSpuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<TradeTrademarkCategoryUserSpuOrderBean> elements, Collector<TradeTrademarkCategoryUserSpuOrderBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        for (TradeTrademarkCategoryUserSpuOrderBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
//                            System.out.println("stt = " + stt);
                            element.setOrderCount((long) (element.getOrderIdSet().size()));
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        );
        reducedStream.print("reducedStream >>> ");

        // TODO 13. 写出到 OLAP 数据库
        SinkFunction<TradeTrademarkCategoryUserSpuOrderBean> jdbcSink =
                ClickHouseUtil.<TradeTrademarkCategoryUserSpuOrderBean>getJdbcSink(
                        "insert into dws_trade_trademark_category_user_spu_order_window values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                );
        reducedStream.<TradeTrademarkCategoryUserSpuOrderBean>addSink(jdbcSink);

        env.execute();
    }
}
