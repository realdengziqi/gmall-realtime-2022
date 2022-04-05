package com.atguigu.gmall.realtime.bean;
import lombok.Data;

/**
 * description:
 * Created by 铁盾 on 2022/3/23
 */

@Data
public class TableProcess {
    //动态分流Sink常量
//    public static final String SINK_TYPE_HBASE = "hbase";

    //来源表
    String sourceTable;

    //输出类型 hbase kafka
//    String sinkType;

    //输出表(主题)
    String sinkTable;
    //输出字段
    String sinkColumns;
    //主键字段
    String sinkPk;
    //建表扩展
    String sinkExtend;
}

