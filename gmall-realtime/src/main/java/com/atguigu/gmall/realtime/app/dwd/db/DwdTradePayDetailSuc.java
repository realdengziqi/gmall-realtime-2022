package com.atguigu.gmall.realtime.app.dwd.db;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.realtime.bean.PaymentSucBean;
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

import java.time.ZoneId;
import java.util.Map;
import java.util.Set;

/**
 * description:
 * Created by 铁盾 on 2022/3/25
 */
public class DwdTradePayDetailSuc {
    public static void main(String[] args) throws Exception {
        // TODO 1. 基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));

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

        // TODO 3. 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table topic_db(\n" +
                "`database` string,\n" +
                "`table` string,\n" +
                "`type` string,\n" +
                "`data` map<string, string>,\n" +
                "`old` string,\n" +
                "`proc_time` as PROCTIME(),\n" +
                "`ts` string\n" +
                ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_trade_pay_detail_suc"));

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

        // TODO 5. 读取订单表数据
        Table orderInfo = tableEnv.sqlQuery("select\n" +
                "data['id'] id,\n" +
                "data['province_id'] province_id\n" +
                "from topic_db\n" +
                "where `table` = 'order_info'\n" +
                "and (`type` = 'insert'\n" +
                "or `type` = 'bootstrap-insert')");
        tableEnv.createTemporaryView("order_info", orderInfo);

        // TODO 6. 读取订单明细活动关联表数据
        Table orderDetailActivity = tableEnv.sqlQuery("select \n" +
                "data['order_detail_id'] order_detail_id,\n" +
                "data['activity_id'] activity_id,\n" +
                "data['activity_rule_id'] activity_rule_id\n" +
                "from `topic_db`\n" +
                "where `table` = 'order_detail_activity'\n" +
                "and (`type` = 'insert'\n" +
                "or `type` = 'bootstrap-insert')");
        tableEnv.createTemporaryView("order_detail_activity", orderDetailActivity);

        // TODO 7. 读取订单明细优惠券关联表数据
        Table orderDetailCoupon = tableEnv.sqlQuery("select\n" +
                "data['order_detail_id'] order_detail_id,\n" +
                "data['coupon_id'] coupon_id\n" +
                "from `topic_db`\n" +
                "where `table` = 'order_detail_coupon'\n" +
                "and (`type` = 'insert'" +
                "or `type` = 'bootstrap-insert')");
        tableEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);

        // TODO 8. 读取支付表数据并转化为流
        // 8.1 读取支付表数据
        Table paymentInfo = tableEnv.sqlQuery("select\n" +
                "data['user_id'] user_id,\n" +
                "data['order_id'] order_id,\n" +
                "data['payment_type'] payment_type,\n" +
                "data['callback_time'] callback_time,\n" +
                "`old`,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'payment_info'\n" +
                "and (`type` = 'update'\n" +
                "or `type` = 'bootstrap-insert')\n" +
                "and data['payment_status']='1602'");

        // 8.2 转化为流
        DataStream<PaymentSucBean> paymentSucDS = tableEnv.toAppendStream(paymentInfo, PaymentSucBean.class);

        // TODO 9. 过滤支付成功数据
        SingleOutputStreamOperator<PaymentSucBean> filteredDS = paymentSucDS.filter(
                paymentSuc -> {
                    String old = paymentSuc.getOld();
                    if(old != null) {
                        Map oldMap = JSON.parseObject(old, Map.class);
                        Set changeKeys = oldMap.keySet();
                        return changeKeys.contains("payment_status");
                    }
                    return true;
                }
        );

        // TODO 10. 将支付成功流转化为支付成功表
        Table paymentSuc = tableEnv.fromDataStream(filteredDS);
        tableEnv.createTemporaryView("payment_suc", paymentSuc);

        // TODO 11. 建立 MySQL-LookUp 字典表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        // TODO 12. 关联 7 张表获得支付成功宽表
        Table resultTable = tableEnv.sqlQuery("select\n" +
                "od.id,\n" +
                "od.order_id,\n" +
                "pay_suc.user_id,\n" +
                "od.sku_id,\n" +
                "oi.province_id,\n" +
                "act.activity_id,\n" +
                "act.activity_rule_id,\n" +
                "cou.coupon_id,\n" +
                "pay_suc.payment_type,\n" +
                "pay_dic.dic_name,\n" +
                "date_format(pay_suc.callback_time,'yyyy-MM-dd') date_id,\n" +
                "pay_suc.callback_time,\n" +
                "od.source_id,\n" +
                "od.source_type,\n" +
                "src_dic.dic_name,\n" +
                "od.sku_num,\n" +
                "od.split_original_amount,\n" +
                "od.split_activity_amount,\n" +
                "od.split_coupon_amount,\n" +
                "od.split_total_amount,\n" +
                "pay_suc.ts,\n" +
                "current_row_timestamp() row_op_ts\n" +
                "from order_detail od\n" +
                "join\n" +
                "payment_suc pay_suc\n" +
                "on od.order_id = pay_suc.order_id\n" +
                "left join \n" +
                "order_info oi\n" +
                "on od.order_id = oi.id\n" +
                "left join order_detail_activity act\n" +
                "on od.id = act.order_detail_id\n" +
                "left join order_detail_coupon cou\n" +
                "on od.id = cou.order_detail_id\n" +
                "left join \n" +
                "base_dic for system_time as of od.proc_time as pay_dic\n" +
                "on pay_suc.payment_type = pay_dic.dic_code\n" +
                "left join \n" +
                "base_dic for system_time as of od.proc_time as src_dic\n" +
                "on od.source_type = src_dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);
//        tableEnv.sqlQuery("select * from result_table limit 10")
//                .execute()
//                .print();

        // TODO 13. 创建 Upsert-Kafka dwd_trade_pay_detail 表
        tableEnv.executeSql("create table dwd_trade_pay_detail_suc(\n" +
                "id string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "province_id string,\n" +
                "activity_id string,\n" +
                "activity_rule_id string,\n" +
                "coupon_id string,\n" +
                "payment_type_code string,\n" +
                "payment_type_name string,\n" +
                "date_id string,\n" +
                "callback_time string,\n" +
                "source_id string,\n" +
                "source_type_code string,\n" +
                "source_type_name string,\n" +
                "sku_num string,\n" +
                "split_original_amount string,\n" +
                "split_activity_amount string,\n" +
                "split_coupon_amount string,\n" +
                "split_payment_amount string,\n" +
                "ts string,\n" +
                "row_op_ts timestamp_ltz(3),\n" +
                "primary key(id) not enforced\n" +
                ")" + KafkaUtil.getUpsertKafkaDDL("dwd_trade_pay_detail_suc"));

        // TODO 14. 将关联结果写入 Upsert-Kafka 表
        tableEnv.executeSql("" +
                "insert into dwd_trade_pay_detail_suc select * from result_table");

        env.execute();
    }
}
