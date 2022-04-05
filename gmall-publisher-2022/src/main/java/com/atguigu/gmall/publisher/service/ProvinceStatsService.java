package com.atguigu.gmall.publisher.service;

import com.atguigu.gmall.publisher.beans.ProvinceStats;

import java.util.List;

/**
 * Author: Felix
 * Date: 2022/1/10
 * Desc: 地区主题统计Service接口
 */
public interface ProvinceStatsService {
    List<ProvinceStats> getProvinceStats(Integer date);
}
