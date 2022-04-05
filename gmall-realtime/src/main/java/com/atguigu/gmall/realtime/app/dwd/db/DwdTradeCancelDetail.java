package com.atguigu.gmall.realtime.app.dwd.db;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.realtime.bean.OrderCancelBean;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import com.atguigu.gmall.realtime.util.MysqlUtil;
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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * description:
 * Created by 铁盾 on 2022/3/25
 */
public class DwdTradeCancelDetail {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 状态后端设置
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
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

        // TODO 3. 读取 Kafka topic_db 主题，封装为 Flink SQL 表
        tableEnv.executeSql("" +
                "create table topic_db(\n" +
                "`database` string,\n" +
                "`table` string,\n" +
                "`data` map<string, string>,\n" +
                "`type` string,\n" +
                "`old` string,\n" +
                "`proc_time` as PROCTIME(),\n" +
                "`ts` string\n" +
                ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_trade_cancel_detail")
        );

        // TODO 4. 读取订单明细表数据
        Table orderDetail = tableEnv.sqlQuery("select\n" +
                "data['id'] id,\n" +
                "data['order_id'] order_id,\n" +
                "data['sku_id'] sku_id,\n" +
                "data['source_id'] source_id,\n" +
                "data['source_type'] source_type,\n" +
                "data['sku_num'] sku_num,\n" +
                "cast(cast(data['sku_num'] as int) * cast(data['order_price'] as int) as string) split_original_amount,\n" +
                "data['split_total_amount'] split_total_amount,\n" +
                "data['split_activity_amount'] split_activity_amount,\n" +
                "data['split_coupon_amount'] split_coupon_amount,\n" +
                "proc_time\n" +
                "from topic_db\n" +
                "where `table` = 'order_detail'\n" +
                "and (`type` = 'insert'\n" +
                "or `type` = 'bootstrap-insert')");
        tableEnv.createTemporaryView("order_detail", orderDetail);

        // TODO 5. 读取订单明细活动关联表数据
        Table orderDetailActivity = tableEnv.sqlQuery("select\n" +
                "data['order_detail_id'] order_detail_id,\n" +
                "data['activity_id'] activity_id,\n" +
                "data['activity_rule_id'] activity_rule_id\n" +
                "from topic_db\n" +
                "where `table` = 'order_detail_activity'\n" +
                "and (`type` = 'insert'\n" +
                "or `type` = 'bootstrap-insert')");
        tableEnv.createTemporaryView("order_detail_activity", orderDetailActivity);

        // TODO 6. 读取订单明细优惠券关联表数据
        Table orderDetailCoupon = tableEnv.sqlQuery("select\n" +
                "data['order_detail_id'] order_detail_id,\n" +
                "data['coupon_id'] coupon_id\n" +
                "from topic_db\n" +
                "where `table` = 'order_detail_coupon'\n" +
                "and (`type` = 'insert'\n" +
                "or `type` = 'bootstrap-insert')");
        tableEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);

        // TODO 7. 读取订单表数据并转化为流
        // 7.1 读取订单表数据
        Table orderInfo = tableEnv.sqlQuery("select\n" +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['province_id'] province_id,\n" +
                "data['operate_time'] cancel_time,\n" +
                "`old`,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'order_info'\n" +
                "and (type = 'update'\n" +
                "or `type` = 'bootstrap-insert')\n" +
                "and data['order_status']='1003'");

        // 7.2 转化为流
        DataStream<OrderCancelBean> orderCancelDS = tableEnv.toAppendStream(orderInfo, OrderCancelBean.class);

        // TODO 8. 过滤符合条件的退单数据
        SingleOutputStreamOperator<OrderCancelBean> filteredDS = orderCancelDS.filter(
                orderCancel -> {
                    String old = orderCancel.getOld();
                    if(old != null) {
                        Map oldMap = JSON.parseObject(old, Map.class);
                        Set<String> changeKeys = oldMap.keySet();
                        return changeKeys.contains("order_status");
                    }
                    return true;
                }
        );

        // TODO 9. 将退单流转化为表
        Table orderCancel = tableEnv.fromDataStream(filteredDS);
        tableEnv.createTemporaryView("order_cancel", orderCancel);
//        tableEnv.sqlQuery("select * from order_cancel limit 10")
//                .execute()
//                .print();

        // TODO 10. 建立 MySQL-LookUp 字典表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        // TODO 11. 关联五张表，获得取消订单宽表
        Table resultTable = tableEnv.sqlQuery("select\n" +
                "od.id,\n" +
                "od.order_id,\n" +
                "oc.user_id,\n" +
                "od.sku_id,\n" +
                "oc.province_id,\n" +
                "act.activity_id,\n" +
                "act.activity_rule_id,\n" +
                "cou.coupon_id,\n" +
                "date_format(oc.cancel_time,'yyyy-MM-dd') date_id,\n" +
                "oc.cancel_time,\n" +
                "od.source_id,\n" +
                "od.source_type,\n" +
                "dic.dic_name,\n" +
                "od.sku_num,\n" +
                "od.split_original_amount,\n" +
                "od.split_activity_amount,\n" +
                "od.split_coupon_amount,\n" +
                "od.split_total_amount,\n" +
                "oc.ts,\n" +
                "current_row_timestamp() row_op_ts\n" +
                "from order_detail od\n" +
                "join \n" +
                "order_cancel oc\n" +
                "on od.order_id = oc.id\n" +
                "left join\n" +
                "order_detail_activity act\n" +
                "on od.id = act.order_detail_id\n" +
                "left join\n" +
                "order_detail_coupon cou\n" +
                "on od.id = cou.order_detail_id\n" +
                "left join \n" +
                "base_dic for system_time as of od.proc_time as dic\n" +
                "on od.source_type = dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 12. 建立 Upsert-Kafka dwd_trade_cart_add 表
        tableEnv.executeSql("create table dwd_trade_cancel_detail(\n" +
                "id string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "province_id string,\n" +
                "activity_id string,\n" +
                "activity_rule_id string,\n" +
                "coupon_id string,\n" +
                "date_id string,\n" +
                "cancel_time string,\n" +
                "source_id string,\n" +
                "source_type_code string,\n" +
                "source_type_name string,\n" +
                "sku_num string,\n" +
                "split_original_amount string,\n" +
                "split_activity_amount string,\n" +
                "split_coupon_amount string,\n" +
                "split_total_amount string,\n" +
                "ts string,\n" +
                "row_op_ts timestamp_ltz(3),\n" +
                "primary key(id) not enforced\n" +
                ")" + KafkaUtil.getUpsertKafkaDDL("dwd_trade_cancel_detail"));

        // TODO 13. 将关联结果写入 Upsert-Kafka 表
        tableEnv.executeSql("" +
                "insert into dwd_trade_cancel_detail select * from result_table");

        env.execute();
    }
}
