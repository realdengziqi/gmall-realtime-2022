package com.atguigu.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.MyBroadcastFunction;
import com.atguigu.gmall.realtime.app.func.MyPhoenixSink;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * description:
 * Created by 铁盾 on 2022/3/15
 */
public class DimSinkApp {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // TODO 2. 状态后端设置
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(
//                10, Time.of(1L, TimeUnit.DAYS), Time.of(3L, TimeUnit.MINUTES)
//        ));
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3. 读取业务主流
        String topic = "topic_db";
        String groupId = "dim_sink_app";
        DataStreamSource<String> gmallDS = env.addSource(KafkaUtil.getKafkaConsumer(topic, groupId));

        // TODO 4. 主流数据结构转换
        SingleOutputStreamOperator<JSONObject> jsonDS = gmallDS.map(JSON::parseObject);

        // TODO 5. 主流 ETL
        SingleOutputStreamOperator<JSONObject> filterDS = jsonDS.filter(
                jsonObj ->
                {
                    try {
                        jsonObj.getJSONObject("data");
                        if(jsonObj.getString("type").equals("bootstrap-start")
                        || jsonObj.getString("type").equals("bootstrap-complete")) {
                            return false;
                        }
                        return true;
                    } catch (JSONException jsonException) {
                        return false;
                    }
                });

        filterDS.print("filterDS >>> ");

        // TODO 6. FlinkCDC 读取配置流并广播流
        // 6.1 FlinkCDC 读取配置表信息
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall_config") // set captured database
                .tableList("gmall_config.table_process") // set captured table
                .username("root")
                .password("000000")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .startupOptions(StartupOptions.initial())
                .build();

        // 6.2 封装为流
        DataStreamSource<String> mysqlDSSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MysqlSource");
//        mysqlDSSource.print("mysqlDSSource>>>").setParallelism(1);

        // 6.3 广播配置流
        MapStateDescriptor<String, TableProcess> tableConfigDescriptor = new MapStateDescriptor<String, TableProcess>("table-process-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastDS = mysqlDSSource.broadcast(tableConfigDescriptor);

        // TODO 7. 连接流
        BroadcastConnectedStream<JSONObject, String> connectedStream = filterDS.connect(broadcastDS);

        // TODO 8. 处理维度表数据
        SingleOutputStreamOperator<JSONObject> dimDS = connectedStream.process(
                new MyBroadcastFunction(tableConfigDescriptor)
        );
        dimDS.print("dimDS >>> ");

        // TODO 9. 将数据写入 Phoenix 表
        dimDS.addSink(new MyPhoenixSink());

        env.execute();
    }
}
