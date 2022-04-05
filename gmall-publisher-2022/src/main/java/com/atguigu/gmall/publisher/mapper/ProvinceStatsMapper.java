package com.atguigu.gmall.publisher.mapper;

import com.atguigu.gmall.publisher.beans.ProvinceStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * Author: Felix
 * Date: 2022/1/10
 * Desc: 地区主题统计Mapper
 */
public interface ProvinceStatsMapper {
    //获取某一天地区的交易额
    @Select("select\n" +
            "province_id,\n" +
            "province_name,\n" +
            "sum(order_amount) order_amount\n" +
            "from dws_trade_province_order_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by province_id, province_name")
    List<ProvinceStats> selectProvinceStats(Integer date);
}
