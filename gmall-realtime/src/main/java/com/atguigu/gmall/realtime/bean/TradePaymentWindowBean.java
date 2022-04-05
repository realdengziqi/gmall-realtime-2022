package com.atguigu.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * description:
 * Created by 铁盾 on 2022/3/31
 */
@Data
@AllArgsConstructor
public class TradePaymentWindowBean {
    // 窗口起始时间
    String stt;

    // 窗口终止时间
    String edt;

    // 支付成功独立用户数
    Long paymentSucUniqueUserCount;

    // 支付成功新用户数
    Long paymentSucNewUserCount;

    // 时间戳
    Long ts;
}
