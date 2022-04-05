package com.atguigu.gmall.realtime.util;

/**
 * description:
 * Created by 铁盾 on 2022/3/24
 */
public class MysqlUtil {

    public static String getBaseDicLookUpDDL() {
        return "create table `base_dic`(\n" +
                "`dic_code` string,\n" +
                "`dic_name` string,\n" +
                "`parent_code` string,\n" +
                "`create_time` timestamp,\n" +
                "`operate_time` timestamp,\n" +
                "primary key(`dic_code`) not enforced\n" +
                ")" + MysqlUtil.mysqlLookUpTableDDL("base_dic");
    }

    public static String mysqlLookUpTableDDL(String tableName) {
        String ddl = "WITH (\n" +
                "'connector' = 'jdbc',\n" +
                "'url' = 'jdbc:mysql://hadoop102:3306/gmall',\n" +
                "'table-name' = '" + tableName + "',\n" +
                "'lookup.cache.max-rows' = '10',\n" +
                "'lookup.cache.ttl' = '1 hour',\n" +
                "'username' = 'root',\n" +
                "'password' = '000000',\n" +
                "'driver' = 'com.mysql.cj.jdbc.Driver'\n" +
                ")";
        return ddl;
    }
}
