package com.atguigu.gmall.realtime.bean;

import lombok.Data;

/**
 * description:
 * Created by 铁盾 on 2022/3/25
 */
@Data
public class CouponUsePayBean {
    // 优惠券领用记录 id
    String id;

    // 优惠券 id
    String coupon_id;

    // 用户 id
    String user_id;

    // 订单 id
    String order_id;

    // 优惠券使用日期（支付）
    String date_id;

    // 优惠券使用时间（支付）
    String used_time;

    // 历史数据
    String old;

    // 时间戳
    String ts;
}
