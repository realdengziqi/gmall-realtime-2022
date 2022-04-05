package com.atguigu.gmall.publisher.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * Author: Felix
 * Date: 2022/1/10
 * Desc: 地区交易额统计实体类
 */
@AllArgsConstructor
@Data
@NoArgsConstructor
public class ProvinceStats {
    private String stt;
    private String edt;
    private String province_id;
    private String province_name;
    private BigDecimal order_amount;
    private String ts;
}

