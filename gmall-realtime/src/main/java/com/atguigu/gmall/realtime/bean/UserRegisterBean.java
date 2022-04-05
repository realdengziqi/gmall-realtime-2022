package com.atguigu.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * description:
 * Created by 铁盾 on 2022/3/30
 */
@Data
@AllArgsConstructor
public class UserRegisterBean {
    // 窗口起始时间
    String stt;
    // 窗口终止时间
    String edt;
    // 注册用户数
    Long registerCt;
    // 时间戳
    Long ts;
}
