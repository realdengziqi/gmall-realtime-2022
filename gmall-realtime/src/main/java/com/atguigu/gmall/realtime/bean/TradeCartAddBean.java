package com.atguigu.gmall.realtime.bean;

import lombok.Data;

/**
 * description:
 * Created by 铁盾 on 2022/4/1
 */
@Deprecated
@Data
public class TradeCartAddBean {
    String id;
    String user_id;
    String sku_id;
    String date_id;
    String create_time;
    String source_id;
    String source_type;
    String source_type_name;
    String sku_num;
    String ts;
}
