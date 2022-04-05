package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.publisher.beans.KeywordStats;
import com.atguigu.gmall.publisher.mapper.KeywordStatsMapper;
import com.atguigu.gmall.publisher.service.KeywordStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Author: Felix
 * Date: 2022/1/11
 * Desc: 关键词主题统计Service接口实现类
 */
@Service
public class KeywordStatsServiceImpl implements KeywordStatsService {

    @Autowired
    private KeywordStatsMapper keywordStatsMapper;

    @Override
    public List<KeywordStats> getKeywordStats(Integer date, Integer limit) {
        return keywordStatsMapper.selectKeywordStats(date,limit);
    }
}
