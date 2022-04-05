package com.atguigu.gmall.publisher.mapper;

import com.atguigu.gmall.publisher.beans.VisitorStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * Author: Felix
 * Date: 2022/1/10
 * Desc: 访客主题统计Mapper接口
 */
public interface
VisitorStatsMapper {

    @Select("select\n" +
            "is_new,\n" +
            "sum(uv_ct) uv_ct,\n" +
            "sum(pv_ct) pv_ct,\n" +
            "sum(sv_ct) sv_ct,\n" +
            "sum(uj_ct) uj_ct,\n" +
            "sum(dur_sum) dur_sum\n" +
            "from dws_traffic_channel_page_view_window\n" +
            "where toYYYYMMDD(stt) =#{date}\n" +
            "group by is_new")
    List<VisitorStats> selectVisitorStatsByIsnew(Integer date);


    @Select("select\n" +
            "toHour(stt) hr,\n" +
            "sum(uv_ct)  uv_ct,\n" +
            "sum(pv_ct)  pv_ct,\n" +
            "sum(if(is_new = '1', dws_traffic_channel_page_view_window.uv_ct, 0)) new_uv\n" +
            "from dws_traffic_channel_page_view_window\n" +
            "where toYYYYMMDD(stt) =#{date}\n" +
            "group by hr")
    List<VisitorStats> selectVisitorStatsByHr(Integer date);


}
