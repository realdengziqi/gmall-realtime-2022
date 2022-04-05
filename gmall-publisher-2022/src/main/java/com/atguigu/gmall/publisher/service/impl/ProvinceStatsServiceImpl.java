package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.publisher.beans.ProvinceStats;
import com.atguigu.gmall.publisher.mapper.ProvinceStatsMapper;
import com.atguigu.gmall.publisher.service.ProvinceStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Author: Felix
 * Date: 2022/1/10
 * Desc: 地区主题统计Service接口实现类
 */
@Service
public class ProvinceStatsServiceImpl implements ProvinceStatsService {

    @Autowired
    private ProvinceStatsMapper provinceStatsMapper;

    @Override
    public List<ProvinceStats> getProvinceStats(Integer date) {
        return provinceStatsMapper.selectProvinceStats(date);
    }
}
