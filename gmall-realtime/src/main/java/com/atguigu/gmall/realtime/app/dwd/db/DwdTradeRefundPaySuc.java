package com.atguigu.gmall.realtime.app.dwd.db;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.realtime.bean.OrderInfoRefundPayBean;
import com.atguigu.gmall.realtime.bean.OrderRefundInfoBean;
import com.atguigu.gmall.realtime.bean.RefundPaymentBean;
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
public class DwdTradeRefundPaySuc {
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

        // TODO 3. 从 Kafka 读取 topic_db 数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table topic_db(" +
                "`database` string,\n" +
                "`table` string,\n" +
                "`type` string,\n" +
                "`data` map<string, string>,\n" +
                "`old` string,\n" +
                "`proc_time` as PROCTIME(),\n" +
                "`ts` string\n" +
                ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_trade_refund_pay_suc"));

        // TODO 4. 建立 MySQL-LookUp 字典表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        // TODO 5. 读取退款表数据并与字典表关联，再将关联后的表转化为流
        // 5.1 读取退款表数据
        Table refundPaymentOrigin = tableEnv.sqlQuery("select\n" +
                        "data['id'] id,\n" +
                        "data['order_id'] order_id,\n" +
                        "data['sku_id'] sku_id,\n" +
                        "data['payment_type'] payment_type,\n" +
                        "data['callback_time'] callback_time,\n" +
                        "data['total_amount'] total_amount,\n" +
                        "`old`,\n" +
                        "proc_time,\n" +
                        "ts\n" +
//                        ",`type` \n" +
                        "from topic_db\n" +
                        "where `table` = 'refund_payment'\n" +
                        "and (`type` = 'update'\n" +
                        "or `type` = 'bootstrap-insert')\n" +
                        "and data['refund_status']='0701'"
        );
        tableEnv.createTemporaryView("refund_payment_origin", refundPaymentOrigin);

        // 5.2 关联退款表和字典表
        Table refundPayment = tableEnv.sqlQuery("select \n" +
                "rpo.id,\n" +
                "rpo.order_id,\n" +
                "rpo.sku_id,\n" +
                "rpo.payment_type,\n" +
                "dic.dic_name payment_type_name,\n" +
                "rpo.callback_time,\n" +
                "rpo.total_amount,\n" +
                "rpo.`old`,\n" +
                "rpo.ts\n" +
                "from refund_payment_origin rpo\n" +
                "left join \n" +
                "base_dic for system_time as of rpo.proc_time as dic\n" +
                "on rpo.payment_type = dic.dic_code\n");
        tableEnv.createTemporaryView("refund_payment_joined", refundPayment);
//        tableEnv.sqlQuery("" +
//                "select * from refund_payment_joined limit 10")
//                .execute()
//                .print();

        // 5.3 将关联后的表转化为流
        DataStream<RefundPaymentBean> refundPaymentDS = tableEnv.toAppendStream(refundPayment, RefundPaymentBean.class);

        // TODO 5. 过滤符合条件的退款表数据并封装为表
        SingleOutputStreamOperator<RefundPaymentBean> refundPaymentFilteredDS = refundPaymentDS.filter(
                refundPaymentBean -> {
                    String old = refundPaymentBean.getOld();
                    if (old != null) {
                        Map oldMap = JSON.parseObject(old, Map.class);
                        Set changeKeys = oldMap.keySet();
                        return changeKeys.contains("refund_status");
                    }
                    return true;
                }
        );
        Table refundPaymentFiltered = tableEnv.fromDataStream(refundPaymentFilteredDS);
        tableEnv.createTemporaryView("refund_payment", refundPaymentFiltered);

        // TODO 6. 读取订单表数据并封装为流
        Table orderInfo = tableEnv.sqlQuery("select\n" +
                        "data['id'] id,\n" +
                        "data['user_id'] user_id,\n" +
                        "data['province_id'] province_id,\n" +
                        "`old`\n" +
                        "from topic_db\n" +
                        "where `table` = 'order_info'\n" +
                        "and (`type` = 'update'\n" +
                        "or `type` = 'bootstrap-insert')\n"
//                +
//                "and data['order_status']='1006'"
        );
        DataStream<OrderInfoRefundPayBean> orderInfoRefundPayDS = tableEnv.toAppendStream(orderInfo, OrderInfoRefundPayBean.class);

        // TODO 7. 过滤符合条件的订单表数据并封装为表
        SingleOutputStreamOperator<OrderInfoRefundPayBean> orderInfoFilteredDS = orderInfoRefundPayDS.filter(
                orderInfoRefundPay -> {
                    String old = orderInfoRefundPay.getOld();
                    if(old != null) {
                        Map oldMap = JSON.parseObject(old, Map.class);
                        Set changeKeys = oldMap.keySet();
                        return changeKeys.contains("order_status");
                    }
                    return true;
                }
        );
        Table orderInfoFiltered = tableEnv.fromDataStream(orderInfoFilteredDS);
        tableEnv.createTemporaryView("order_info", orderInfoFiltered);

        // TODO 8. 读取退单表数据并封装为流
        Table orderRefundInfo = tableEnv.sqlQuery("select\n" +
                        "data['order_id'] order_id,\n" +
                        "data['sku_id'] sku_id,\n" +
                        "data['refund_num'] refund_num,\n" +
                        "`old`\n" +
                        "from topic_db\n" +
                        "where `table` = 'order_refund_info'\n" +
                        "and (`type` = 'update'\n" +
                        "or `type` = 'bootstrap-insert')\n"
//                +
//                "and data['refund_status']='1006'"
        );
        tableEnv.createTemporaryView("order_refund_info_origin", orderRefundInfo);
//        tableEnv.sqlQuery("select * from order_refund_info_origin")
//                .execute()
//                .print();
        DataStream<OrderRefundInfoBean> orderRefundInfoDS = tableEnv.toAppendStream(orderRefundInfo, OrderRefundInfoBean.class);

        // TODO 9. 过滤符合条件的退单表数据并封装为表
        SingleOutputStreamOperator<OrderRefundInfoBean> orderRefundInfoFilteredDS = orderRefundInfoDS.filter(
                orderRefundInfoBean -> {
                    String old = orderRefundInfoBean.getOld();
                    if (old != null) {
                        Map oldMap = JSON.parseObject(old, Map.class);
                        Set changeKeys = oldMap.keySet();
                        return changeKeys.contains("refund_status");
                    }
                    return true;
                }
        );
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfoFilteredDS);

        // TODO 11. 关联三张表获得退款成功表
        Table resultTable = tableEnv.sqlQuery("select\n" +
                "rp.id,\n" +
                "oi.user_id,\n" +
                "rp.order_id,\n" +
                "rp.sku_id,\n" +
                "oi.province_id,\n" +
                "rp.payment_type,\n" +
                "rp.payment_type_name,\n" +
                "date_format(rp.callback_time,'yyyy-MM-dd') date_id,\n" +
                "rp.callback_time,\n" +
                "ri.refund_num,\n" +
                "rp.total_amount,\n" +
                "rp.ts,\n" +
                "current_row_timestamp() row_op_ts\n" +
                "from refund_payment rp \n" +
                "left join \n" +
                "order_info oi\n" +
                "on rp.order_id = oi.id\n" +
                "left join\n" +
                "order_refund_info ri\n" +
                "on rp.order_id = ri.order_id\n" +
                "and rp.sku_id = ri.sku_id");
        tableEnv.createTemporaryView("result_table", resultTable);
//        tableEnv.sqlQuery("select * from " +
//                "result_table limit 10")
//                .execute()
//                .print();

        // TODO 12. 创建 Upsert-Kafka dwd_trade_refund_pay_suc 表
        tableEnv.executeSql("create table dwd_trade_refund_pay_suc(\n" +
                "id string,\n" +
                "user_id string,\n" +
                "order_id string,\n" +
                "sku_id string,\n" +
                "province_id string,\n" +
                "payment_type_code string,\n" +
                "payment_type_name string,\n" +
                "date_id string,\n" +
                "callback_time string,\n" +
                "refund_num string,\n" +
                "refund_amount string,\n" +
                "ts string,\n" +
                "row_op_ts timestamp_ltz(3),\n" +
                "primary key(id) not enforced\n" +
                ")" + KafkaUtil.getUpsertKafkaDDL("dwd_trade_refund_pay_suc"));

        // TODO 13. 将关联结果写入 Upsert-Kafka 表
        tableEnv.executeSql("" +
                "insert into dwd_trade_refund_pay_suc select * from result_table");

        env.execute();
    }
}
