package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.publisher.beans.VisitorStats;
import com.atguigu.gmall.publisher.mapper.VisitorStatsMapper;
import com.atguigu.gmall.publisher.service.VisitorStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Author: Felix
 * Date: 2022/1/10
 * Desc: 访客主题统计Service接口实现类
 */
@Service
public class VisitorStatsServiceImpl implements VisitorStatsService {
    @Autowired
    private VisitorStatsMapper visitorStatsMapper;
    @Override
    public List<VisitorStats> getVisitorStatsByIsnew(Integer date) {
        return visitorStatsMapper.selectVisitorStatsByIsnew(date);
    }

    @Override
    public List<VisitorStats> getVisitorStatsByHr(Integer date) {
        return visitorStatsMapper.selectVisitorStatsByHr(date);
    }
}
