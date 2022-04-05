package com.atguigu.gmall.realtime.bean;

import lombok.Data;

/**
 * description:
 * Created by 铁盾 on 2022/3/25
 */
@Data
public class OrderRefundInfoBean {
    // 订单 id
    String order_id;

    // sku_id
    String sku_id;

    // 退货件数
    String refund_num;

    // 历史数据
    String old;
}
