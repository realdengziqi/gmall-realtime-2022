package com.atguigu.gmall.publisher.service;

import com.atguigu.gmall.publisher.beans.VisitorStats;

import java.util.List;

/**
 * Author: Felix
 * Date: 2022/1/10
 * Desc: 访客主题统计Service接口
 */
public interface VisitorStatsService {
    List<VisitorStats> getVisitorStatsByIsnew(Integer date);

    List<VisitorStats> getVisitorStatsByHr(Integer date);
}
