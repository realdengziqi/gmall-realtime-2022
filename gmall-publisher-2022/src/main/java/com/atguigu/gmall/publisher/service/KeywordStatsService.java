package com.atguigu.gmall.publisher.service;

import com.atguigu.gmall.publisher.beans.KeywordStats;

import java.util.List;

/**
 * Author: Felix
 * Date: 2022/1/11
 * Desc: 关键词主题统计Service接口
 */
public interface KeywordStatsService {
    List<KeywordStats> getKeywordStats(Integer date,Integer limit);
}
