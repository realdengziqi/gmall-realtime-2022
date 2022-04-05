package com.atguigu.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * description:
 * Created by 铁盾 on 2022/3/30
 */
@Data
@AllArgsConstructor
public class UserLoginBean {
    // 窗口起始时间
    String stt;

    // 窗口终止时间
    String edt;

    // 回流用户数
    Long backCt;

    // 独立用户数
    Long uuCt;

    // 时间戳
    Long ts;
}
