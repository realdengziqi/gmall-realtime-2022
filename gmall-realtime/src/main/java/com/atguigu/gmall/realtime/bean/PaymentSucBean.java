package com.atguigu.gmall.realtime.bean;

import lombok.Data;

/**
 * description:
 * Created by 铁盾 on 2022/3/25
 */
@Data
public class PaymentSucBean {
    // 用户 id
    String user_id;

    // 订单 id
    String order_id;

    // 支付类型
    String payment_type;

    // 回调时间
    String callback_time;

    // 历史数据
    String old;

    // 时间戳
    String ts;
}
