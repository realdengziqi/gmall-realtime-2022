package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.bean.TransientSink;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * description:
 * Created by 铁盾 on 2022/3/29
 */
public class ClickHouseUtil {
    public static <T> SinkFunction<T> getJdbcSink(String sql) {
        return JdbcSink.<T>sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T obj) throws SQLException {
                        Field[] declaredFields = obj.getClass().getDeclaredFields();
                        int skipNum = 0;
                        for (int i = 0; i < declaredFields.length; i++) {
                            Field declaredField = declaredFields[i];
                            TransientSink transientSink = declaredField.getAnnotation(TransientSink.class);
                            if (transientSink != null) {
                                skipNum++;
                                continue;
                            }
                            declaredField.setAccessible(true);
                            try {
                                Object value = declaredField.get(obj);
//                                if(i == 1) {
//                                    System.out.println(value);
//                                }
                                preparedStatement.setObject(i + 1 - skipNum, value);
                            } catch (IllegalAccessException e) {
                                System.out.println("ClickHouse 数据插入异常~");
                                e.printStackTrace();
                            }
                        }
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build()
        );
    }
}
