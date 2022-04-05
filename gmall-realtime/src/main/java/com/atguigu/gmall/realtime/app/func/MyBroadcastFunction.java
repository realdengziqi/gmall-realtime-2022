package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

/**
 * description:
 * Created by 铁盾 on 2022/3/23
 */
public class MyBroadcastFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private MapStateDescriptor<String, TableProcess> tableConfigDescriptor;
    private Connection conn;

    public MyBroadcastFunction(MapStateDescriptor<String, TableProcess> tableConfigDescriptor) {
        this.tableConfigDescriptor = tableConfigDescriptor;
    }

    @Override
    public void open(Configuration parameter) throws Exception {
        super.open(parameter);
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void processElement(JSONObject jsonObj, ReadOnlyContext readOnlyContext, Collector<JSONObject> out) throws Exception {
        ReadOnlyBroadcastState<String, TableProcess> tableConfigState = readOnlyContext.getBroadcastState(tableConfigDescriptor);
        // 获取配置信息
        String sourceTable = jsonObj.getString("table");
        TableProcess tableConfig = tableConfigState.get(sourceTable);
        if (tableConfig != null) {
            JSONObject data = jsonObj.getJSONObject("data");
            String sinkTable = tableConfig.getSinkTable();

            // 根据 sinkColumns 过滤数据
            String sinkColumns = tableConfig.getSinkColumns();
            filterColumns(data, sinkColumns);

            // 将目标表名加入到主流数据中
            data.put("sinkTable", sinkTable);
            System.out.println("tableProcessSinktable" + sinkTable);

            System.out.println("data = " + data);
            out.collect(data);
        }
    }

    private void filterColumns(JSONObject data, String sinkColumns) {
        Set<Map.Entry<String, Object>> dataEntries = data.entrySet();
        dataEntries.removeIf(r -> !sinkColumns.contains(r.getKey()));
    }

    @Override
    public void processBroadcastElement(String jsonStr, Context context, Collector<JSONObject> out) throws Exception {
        JSONObject jsonObj = JSON.parseObject(jsonStr);
        TableProcess config = jsonObj.getObject("after", TableProcess.class);

        String sourceTable = config.getSourceTable();
        String sinkTable = config.getSinkTable();
        String sinkColumns = config.getSinkColumns();
        String sinkPk = config.getSinkPk();
        String sinkExtend = config.getSinkExtend();

        BroadcastState<String, TableProcess> tableConfigState = context.getBroadcastState(tableConfigDescriptor);
        tableConfigState.put(sourceTable, config);

        checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);
    }

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        StringBuilder sql = new StringBuilder();
        sql.append("create table if not exists " + GmallConfig.HBASE_SCHEMA
                + "." + sinkTable + "(\n");
        String[] columnArr = sinkColumns.split(",");
        if (sinkPk == null) {
            sinkPk = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }
        for (int i = 0; i < columnArr.length; i++) {
            sql.append(columnArr[i] + " varchar");
            if (sinkPk.equals(columnArr[i])) {
                sql.append(" primary key");
            }
            if (i < columnArr.length - 1) {
                sql.append(",\n");
            }
        }
        sql.append(")");
        sql.append(sinkExtend);
        String createStatement = sql.toString();
        PreparedStatement preparedSt = null;
        try {
            preparedSt = conn.prepareStatement(createStatement);
            preparedSt.execute();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            System.out.println("建表语句\n" + createStatement + "\n执行异常");
        } finally {
            if (preparedSt != null) {
                try {
                    preparedSt.close();
                } catch (SQLException sqlException) {
                    sqlException.printStackTrace();
                    throw new RuntimeException("数据库操作对象释放异常");
                }
            }
        }
    }
}
