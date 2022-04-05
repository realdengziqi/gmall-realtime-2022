package com.atguigu.gmall.publisher.service;

import com.atguigu.gmall.publisher.beans.ProductStats;

import java.math.BigDecimal;
import java.util.List;

/**
 * Author: Felix
 * Date: 2022/1/8
 * Desc: 商品主题统计Service接口
 */
public interface ProductStatsService {
    BigDecimal getGmv(Integer date);

    List<ProductStats> getProductStatsByTm(Integer date,Integer limit);

    List<ProductStats> getProductStatsByCategory3(Integer date,Integer limit);

    List<ProductStats> getProductStatsBySpu(Integer date,Integer limit);
}
