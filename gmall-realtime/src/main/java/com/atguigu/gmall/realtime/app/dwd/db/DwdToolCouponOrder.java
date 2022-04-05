package com.atguigu.gmall.realtime.app.dwd.db;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.realtime.bean.CouponUseOrderBean;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Map;
import java.util.Set;

/**
 * description:
 * Created by 铁盾 on 2022/3/25
 */
public class DwdToolCouponOrder {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 状态后端设置
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

        // TODO 3. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table `topic_db` (\n" +
                "`database` string,\n" +
                "`table` string,\n" +
                "`data` map<string, string>,\n" +
                "`type` string,\n" +
                "`old` string,\n" +
                "`ts` string\n" +
                ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_tool_coupon_order"));

        // TODO 4. 读取优惠券领用表数据，封装为流
        Table couponUseOrder = tableEnv.sqlQuery("select\n" +
                "data['id'] id,\n" +
                "data['coupon_id'] coupon_id,\n" +
                "data['user_id'] user_id,\n" +
                "data['order_id'] order_id,\n" +
                "date_format(data['using_time'],'yyyy-MM-dd') date_id,\n" +
                "data['using_time'] using_time,\n" +
                "`old`,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'coupon_use'\n" +
                "and (`type` = 'update'\n" +
                "or `type` = 'bootstrap-insert')");
        DataStream<CouponUseOrderBean> couponUseOrderDS = tableEnv.toAppendStream(couponUseOrder, CouponUseOrderBean.class);

        // TODO 5. 过滤满足条件的优惠券下单数据，封装为表
        SingleOutputStreamOperator<CouponUseOrderBean> filteredDS = couponUseOrderDS.filter(
                couponUseOrderBean -> {
                    String old = couponUseOrderBean.getOld();
                    if(old != null) {
                        Map oldMap = JSON.parseObject(old, Map.class);
                        Set changeKeys = oldMap.keySet();
                        return changeKeys.contains("using_time");
                    }
                    return true;
                }
        );
        Table resultTable = tableEnv.fromDataStream(filteredDS);
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 6. 建立 Upsert-Kafka dwd_tool_coupon_order 表
        tableEnv.executeSql("create table dwd_tool_coupon_order(\n" +
                "id string,\n" +
                "coupon_id string,\n" +
                "user_id string,\n" +
                "order_id string,\n" +
                "date_id string,\n" +
                "order_time string,\n" +
                "ts string,\n" +
                "primary key(id) not enforced\n" +
                ")" + KafkaUtil.getUpsertKafkaDDL("dwd_tool_coupon_order"));

        // TODO 7. 将数据写入 Upsert-Kafka 表
        tableEnv.executeSql("" +
                "insert into dwd_tool_coupon_order select " +
                "id,\n" +
                "coupon_id,\n" +
                "user_id,\n" +
                "order_id,\n" +
                "date_id,\n" +
                "using_time order_time,\n" +
                "ts from result_table");

        env.execute();
    }
}
