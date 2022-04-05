package com.atguigu.gmall.publisher.mapper;

import com.atguigu.gmall.publisher.beans.KeywordStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * Author: Felix
 * Date: 2022/1/11
 * Desc: 关键词统计Mapper接口
 */
public interface KeywordStatsMapper {
    @Select("select\n" +
            "keyword,\n" +
            "sum(keyword_count * multiIf(\n" +
            "source = 'SEARCH', 10,\n" +
            "source = 'ORDER', 5,\n" +
            "source = 'CART', 2,\n" +
            "source = 'CLICK', 1, 0\n" +
            "))          keyword_count\n" +
            "from dws_traffic_source_keyword_page_view_window\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by keyword\n" +
            "order by keyword_count desc\n" +
            "limit #{limit}")
    List<KeywordStats> selectKeywordStats(@Param("date") Integer date,@Param("limit") Integer limit);
}
