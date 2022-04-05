package com.atguigu.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * description:
 * Created by 铁盾 on 2022/3/30
 */
@Data
@AllArgsConstructor
@Builder
public class TradeOrderBean {
    // 窗口起始时间
    String stt;

    // 窗口关闭时间
    String edt;

    // 下单独立用户数
    Long orderUniqueUserCount;

    // 下单新用户数
    Long orderNewUserCount;

    // 下单活动减免金额
    Double orderActivityReduceAmount;

    // 下单优惠券减免金额
    Double orderCouponReduceAmount;

    // 下单原始金额
    Double orderOriginalTotalAmount;

    // 时间戳
    Long ts;
}
