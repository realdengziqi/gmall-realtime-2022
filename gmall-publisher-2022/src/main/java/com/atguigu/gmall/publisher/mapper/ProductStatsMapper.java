package com.atguigu.gmall.publisher.mapper;

import com.atguigu.gmall.publisher.beans.ProductStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

/**
 * Author: Felix
 * Date: 2022/1/8
 * Desc: 商品主题统计接口
 */
public interface ProductStatsMapper {
    // 获取某一天的总交易额
    @Select("select \n" +
            "sum(order_amount) order_total_amount\n" +
            "from dws_trade_province_order_window\n" +
            "where toYYYYMMDD(stt) = #{date}")
    BigDecimal selectGmv(Integer date);

    // 获取某一天各品牌交易额
    @Select("select\n" +
            "trademark_id,\n" +
            "trademark_name,\n" +
            "sum(order_amount) order_amount\n" +
            "from dws_trade_trademark_category_user_spu_order_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by trademark_id, trademark_name\n" +
            "having order_amount > 0\n" +
            "order by order_amount desc\n" +
            "limit #{limit}")
    List<ProductStats> selectProductStatsByTm(@Param("date") Integer date, @Param("limit") Integer limit);

    // 获取某一天各品类交易额
    @Select("select \n" +
            "category3_id,\n" +
            "category3_name,\n" +
            "sum(order_amount) order_amount\n" +
            "from dws_trade_trademark_category_user_spu_order_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by category3_id, category3_name\n" +
            "having order_amount > 0\n" +
            "order by order_amount desc\n" +
            "limit #{limit}")
    List<ProductStats> selectProductStatsByCategory(@Param("date") Integer date, @Param("limit") Integer limit);

    // 获取某一天各 SPU 交易额
    @Select("select\n" +
            "spu_id,\n" +
            "spu_name,\n" +
            "sum(order_amount) order_amount,\n" +
            "sum(order_count) order_count\n" +
            "from dws_trade_trademark_category_user_spu_order_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by spu_id, spu_name\n" +
            "having order_amount > 0\n" +
            "order by order_amount desc\n" +
            "limit #{limit}")
    List<ProductStats> selectProductStatsBySpu(@Param("date") Integer date, @Param("limit") Integer limit);
}
