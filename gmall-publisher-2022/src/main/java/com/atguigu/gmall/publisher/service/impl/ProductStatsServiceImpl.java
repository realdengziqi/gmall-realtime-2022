package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.publisher.beans.ProductStats;
import com.atguigu.gmall.publisher.mapper.ProductStatsMapper;
import com.atguigu.gmall.publisher.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * Author: Felix
 * Date: 2022/1/8
 * Desc: 商品主题统计接口的实现类
 */
@Service
public class ProductStatsServiceImpl implements ProductStatsService {

    @Autowired
    private ProductStatsMapper productStatsMapper;

    @Override
    public BigDecimal getGmv(Integer date) {
        return productStatsMapper.selectGmv(date);
    }

    @Override
    public List<ProductStats> getProductStatsByTm(Integer date, Integer limit) {
        return productStatsMapper.selectProductStatsByTm(date,limit);
    }

    @Override
    public List<ProductStats> getProductStatsByCategory3(Integer date, Integer limit) {
        return productStatsMapper.selectProductStatsByCategory(date,limit);
    }

    @Override
    public List<ProductStats> getProductStatsBySpu(Integer date, Integer limit) {
        return productStatsMapper.selectProductStatsBySpu(date,limit);
    }
}
