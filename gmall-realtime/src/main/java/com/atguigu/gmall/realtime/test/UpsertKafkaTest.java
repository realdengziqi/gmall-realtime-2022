package com.atguigu.gmall.realtime.test;

import com.atguigu.gmall.realtime.test.bean.JoinBean;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * description:
 * Created by 铁盾 on 2022/3/28
 */
public class UpsertKafkaTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        DataStreamSource<String> leftSource = env.fromElements("{\n" +
//                "\t\"id\" : \"A\",\n" +
//                "\t\"tag\": \"left\"\n" +
//                "}","{\n" +
//                "\t\"id\" : \"B\",\n" +
//                "\t\"tag\": \"left\"\n" +
//                "}", "{\n" +
//                "\t\"id\" : \"C\",\n" +
//                "\t\"tag\": \"left\"\n" +
//                "}");
//
//        DataStreamSource<String> rightSource = env.fromElements("{\n" +
//                "\t\"id\" : \"A\",\n" +
//                "\t\"tag\": \"right\"\n" +
//                "}", "{\n" +
//                "\t\"id\" : \"B\",\n" +
//                "\t\"tag\": \"right\"\n" +
//                "}", "{\n" +
//                "\t\"id\" : \"C\",\n" +
//                "\t\"tag\": \"right\"\n" +
//                "}");
//
//        FlinkKafkaProducer<String> leftProducer = KafkaUtil.getKafkaProducer("left_table");
//        FlinkKafkaProducer<String> rightProducer = KafkaUtil.getKafkaProducer("right_table");

//        leftSource.addSink(leftProducer);
//        rightSource.addSink(rightProducer);

        tableEnv.executeSql("" +
                "create table left_table(" +
                "id string, \n" +
                "tag string)" + KafkaUtil.getKafkaDDL("left_table", "left"));
        tableEnv.executeSql("" +
                "create table right_table(" +
                "id string, \n" +
                "tag string)" + KafkaUtil.getKafkaDDL("right_table", "right"));
//
//        tableEnv.sqlQuery("select * from left_table")
//                .execute()
//                .print();

        tableEnv.sqlQuery("select * from right_table")
                .execute()
                .print();

        Table joinTable = tableEnv.sqlQuery("select l.id l_id, l.tag tag_left," +
                "r.tag tag_right\n" +
                "from left_table l left join right_table r on l.id=r.id");

        Table leftTable = tableEnv.sqlQuery("select * from left_table");
        tableEnv.createTemporaryView("leftTableOrigin", leftTable);
        tableEnv.executeSql("create table leftTable (\n" +
                "id string,\n" +
                "tag string\n" +
//                "primary key(id) not enforced \n" +
                ")" + KafkaUtil.getSinkKafkaDDL("leftTable"));
        tableEnv.executeSql("insert into leftTable select * from leftTableOrigin");

//        DataStream<JoinBean> appendS = tableEnv.toAppendStream(joinTable, JoinBean.class);
        DataStream<Row> changelogStream = tableEnv.toChangelogStream(joinTable, Schema.newBuilder()
                .column("l_id", "STRING")
                .column("tag_left", "STRING")
                .column("tag_right", "STRING")
                .build());
//        DataStream<Row> dataStream = tableEnv.toDataStream(joinTable);
        DataStream<Tuple2<Boolean, JoinBean>> retractS = tableEnv.toRetractStream(joinTable, JoinBean.class);
//        appendS.print("appendStream>>>");
        changelogStream.print("changelogStream>>>");
//        dataStream.print("dataStream>>>");
        retractS.print("retractS");

//        Table changelogDataStream = tableEnv.fromDataStream(changelogStream, Schema.newBuilder()
//                .column("l_id", "STRING")
//                .column("tag_left", "STRING")
//                .column("tag_right", "STRING")
//                .build());
//
//        changelogDataStream.execute().print();

        Table changelogTable = tableEnv.fromChangelogStream(
                changelogStream,
                Schema.newBuilder()
                        .column("l_id", "STRING")
                        .column("tag_left", "STRING")
                        .column("tag_right", "STRING")
                        .build()
        );
//        changelogTable.execute().print();

        tableEnv.createTemporaryView("change_log_table", joinTable);

        tableEnv.executeSql("" +
                "create table changelog_topic(\n" +
                "l_id string,\n" +
                "tag_left string,\n" +
                "tag_right string,\n" +
                "primary key(l_id) not enforced\n" +
                ")" + KafkaUtil.getUpsertKafkaDDL("changelog_topic"));

//        tableEnv.executeSql("" +
//                "create table normal_sink_topic(\n" +
//                "l_id string,\n" +
//                "tag_left string,\n" +
//                "tag_right string\n" +
//                ",primary key(l_id) not enforced\n" +
//                ")" + KafkaUtil.getSinkKafkaDDL("normal_sink_topic"));

        tableEnv.executeSql("insert into changelog_topic select * from change_log_table");


        env.execute();
    }
}
