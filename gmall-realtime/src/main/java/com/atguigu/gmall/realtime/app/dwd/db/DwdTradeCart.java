package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * description:
 * Created by 铁盾 on 2022/3/25
 */
@Deprecated
public class DwdTradeCart {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 状态后端设置
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

        // TODO 3. 从 kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table `topic_db`(\n" +
                "`database` string,\n" +
                "`table` string,\n" +
                "`data` map<string, string>\n" +
                ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_trade_cart"));

        // TODO 4. 获取购物车信息表数据
        Table cartInfo = tableEnv.sqlQuery("select \n" +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['sku_id'] sku_id,\n" +
                "data['sku_name'] sku_name,\n" +
                "data['sku_num'] sku_num\n" +
                "from topic_db\n" +
                "where `table` = 'cart_info'\n" +
                "and data['is_ordered']='0'");
        tableEnv.createTemporaryView("cart_info", cartInfo);

        // TODO 5. 建立 Upsert-Kafka dwd_trade_cart 表
        tableEnv.executeSql("create table dwd_trade_cart (\n" +
                "id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "sku_name string,\n" +
                "sku_num string,\n" +
                "primary key(id) not enforced\n" +
                ")" + KafkaUtil.getUpsertKafkaDDL("dwd_trade_cart"));

        // TODO 6. 将数据写入 Upsert-Kafka 表
        tableEnv.executeSql("" +
                "insert into dwd_trade_cart select * from cart_info");

        env.execute();
    }
}
