package com.atguigu.gmall.realtime.bean;

import lombok.Data;
import net.minidev.json.JSONObject;

import java.util.Map;

/**
 * description:
 * Created by 铁盾 on 2022/3/25
 */
@Data
public class OrderCancelBean {
    // 订单 id
    String id;

    // 用户 id
    String user_id;

    // 省份 id
    String province_id;

    // 取消订单时间
    String cancel_time;

    // Maxwell 采集的历史数据字段 old
    String old;

    // 时间戳
    String ts;
}
