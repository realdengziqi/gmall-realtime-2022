package com.atguigu.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * description:
 * Created by 铁盾 on 2022/4/4
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordBean {
    // 窗口起始时间
    private String stt;

    // 窗口闭合时间
    private String edt;

    // 关键词来源
    private String source;

    // 关键词
    private String keyword;

    // 关键词出现频次
    private Long keyword_count;

    // 时间戳
    private Long ts;
}
