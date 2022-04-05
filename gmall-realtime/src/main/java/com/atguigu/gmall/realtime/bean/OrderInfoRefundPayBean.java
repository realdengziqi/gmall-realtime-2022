package com.atguigu.gmall.realtime.bean;

import lombok.Data;

/**
 * description:
 * Created by 铁盾 on 2022/3/25
 */
@Data
public class OrderInfoRefundPayBean {
    // 订单 id
    String id;

    // 用户 id
    String user_id;

    // 省份 id
    String province_id;

    // 历史数据
    String old;
}
