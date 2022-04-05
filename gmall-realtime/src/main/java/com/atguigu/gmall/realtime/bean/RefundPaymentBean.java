package com.atguigu.gmall.realtime.bean;

import lombok.Data;

/**
 * description:
 * Created by 铁盾 on 2022/3/25
 */
@Data
public class RefundPaymentBean {
    // 退款支付 id
    String id;

    // 订单 id
    String order_id;

    // sku_id
    String sku_id;

    // 支付类型
    String payment_type;

    // 支付类型名称
    String payment_type_name;

    // 回调时间
    String callback_time;

    // 退款金额
    String total_amount;

    // 历史数据
    String old;

    // 时间戳
    String ts;
}
