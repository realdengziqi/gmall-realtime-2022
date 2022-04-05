package com.atguigu.gmall.publisher.controller;

import com.atguigu.gmall.publisher.beans.KeywordStats;
import com.atguigu.gmall.publisher.beans.ProductStats;
import com.atguigu.gmall.publisher.beans.ProvinceStats;
import com.atguigu.gmall.publisher.beans.VisitorStats;
import com.atguigu.gmall.publisher.service.KeywordStatsService;
import com.atguigu.gmall.publisher.service.ProductStatsService;
import com.atguigu.gmall.publisher.service.ProvinceStatsService;
import com.atguigu.gmall.publisher.service.VisitorStatsService;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.*;

/**
 * Author: Felix
 * Date: 2022/1/8
 * Desc: 大屏展示Controller
 */
@RestController
@RequestMapping("/api/sugar")
public class SugarController {

    @Autowired
    private ProductStatsService productStatsService;

    @Autowired
    private ProvinceStatsService provinceStatsService;

    @Autowired
    private VisitorStatsService visitorStatsService;

    @Autowired
    private KeywordStatsService keywordStatsService;


    @RequestMapping("/keyword")
    public String getKeywordStats(
        @RequestParam(value = "date", defaultValue = "1") Integer date,
        @RequestParam(value = "limit", defaultValue = "10") Integer limit
    ){
        if(date == 1){
            date = now();
        }

        StringBuilder jsonB = new StringBuilder("{\"status\": 0,\"data\": [");
        List<KeywordStats> keywordStatsList = keywordStatsService.getKeywordStats(date, limit);
        for (int i = 0; i < keywordStatsList.size(); i++) {
            KeywordStats keywordStats = keywordStatsList.get(i);
            jsonB.append("{\"name\": \""+keywordStats.getKeyword()+"\",\"value\": "+keywordStats.getKeyword_count()+"}");
            if(i < keywordStatsList.size() - 1){
                jsonB.append(",");
            }
        }
        jsonB.append("]}");
        return jsonB.toString();
    }


    @RequestMapping("/hr")
    public String getVisitorStatsByHr(@RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = now();
        }
        List<VisitorStats> visitorStatsList = visitorStatsService.getVisitorStatsByHr(date);

        //定义一个长度为24的数组去存放每一个小时的访客指标访问情况
        VisitorStats[] visitorStatsArr = new VisitorStats[24];

        //对查询的结果进行遍历，将每一个小时的访问情况放到数组对应的位置
        for (VisitorStats visitorStats : visitorStatsList) {
            visitorStatsArr[visitorStats.getHr()] = visitorStats;
        }
        //准备4个List集合
        List hrList = new ArrayList();
        List uvList = new ArrayList();
        List pvList = new ArrayList();
        List newUvList = new ArrayList();

        //对数组进行遍历
        for (int hr = 0; hr < visitorStatsArr.length; hr++) {
            VisitorStats visitorStats = visitorStatsArr[hr];
            if(visitorStats != null){
                uvList.add(visitorStats.getUv_ct());
                pvList.add(visitorStats.getPv_ct());
                newUvList.add(visitorStats.getNew_uv());
            }else{
                uvList.add(0L);
                pvList.add(0L);
                newUvList.add(0L);
            }
            hrList.add(String.format("%02d",hr));
        }

        String json = "{\"status\":0,\"msg\":\"\",\"data\":" +
            "{\"categories\":[\""+StringUtils.join(hrList,"\",\"")+"\"]," +
            "\"series\":[" +
            "{\"name\":\"PV\",\"data\":["+StringUtils.join(pvList,",")+"]}," +
            "{\"name\":\"UV\",\"data\":["+StringUtils.join(uvList,",")+"]}," +
            "{\"name\":\"NEW_UV\",\"data\":["+StringUtils.join(newUvList,",")+"]}]}}";

        return json;
    }

    @RequestMapping("/is_new")
    public String getVisitorStatsByIsnew(@RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = now();
        }
        List<VisitorStats> visitorStatsList = visitorStatsService.getVisitorStatsByIsnew(date);
        VisitorStats newVisitorStats = new VisitorStats();
        VisitorStats oldVisitorStats = new VisitorStats();
        for (VisitorStats visitorStats : visitorStatsList) {
            if("1".equals(visitorStats.getIs_new())){
                //新访客
                newVisitorStats = visitorStats;
            }else{
                //老访客
                oldVisitorStats = visitorStats;
            }
        }
        //拼接json字符串
        String json = "{\"status\":0,\"data\":{\"total\":5," +
            "\"columns\":[" +
            "{\"name\":\"类别\",\"id\":\"type\"}," +
            "{\"name\":\"新用户\",\"id\":\"new\"}," +
            "{\"name\":\"老用户\",\"id\":\"old\"}]," +
            "\"rows\":[" +
            "{\"type\":\"用户数(人)\",\"new\":"+newVisitorStats.getUv_ct()+",\"old\":"+oldVisitorStats.getUv_ct()+"}," +
            "{\"type\":\"总访问页面数(次)\",\"new\":"+newVisitorStats.getPv_ct()+",\"old\":"+oldVisitorStats.getPv_ct()+"}," +
            "{\"type\":\"跳出率(%)\",\"new\":"+newVisitorStats.getUjRate()+",\"old\":"+oldVisitorStats.getUjRate()+"}," +
            "{\"type\":\"平均在线时长(秒)\",\"new\":"+newVisitorStats.getDurPerSv()+",\"old\":"+oldVisitorStats.getDurPerSv()+"}," +
            "{\"type\":\"平均访问页面数(人次)\",\"new\":"+newVisitorStats.getPvPerSv()+",\"old\":"+oldVisitorStats.getPvPerSv()+"}]}}";

        return json;
    }
    @RequestMapping("/province")
    public String getProvinceStats(@RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = now();
        }

        StringBuilder jsonB = new StringBuilder("{\"status\": 0,\"data\": {\"mapData\": [");
        List<ProvinceStats> provinceStatsList = provinceStatsService.getProvinceStats(date);
        for (int i = 0; i < provinceStatsList.size(); i++) {
            ProvinceStats provinceStats = provinceStatsList.get(i);
            jsonB.append("{\"name\": \"" + provinceStats.getProvince_name() + "\",\"value\": " + provinceStats.getOrder_amount() + "}");
            if(i < provinceStatsList.size() - 1){
                jsonB.append(",");
            }
        }

        jsonB.append("],\"valueName\": \"交易额\"}}");
        return jsonB.toString();
    }

    @RequestMapping("/spu")
    public String getProductStatsBySpu(
        @RequestParam(value = "date", defaultValue = "1") Integer date,
        @RequestParam(value = "limit", defaultValue = "10") Integer limit) {

        if (date == 1) {
            date = now();
        }
        List<ProductStats> productStatsList = productStatsService.getProductStatsBySpu(date, limit);

        StringBuilder jsonB = new StringBuilder(
                "{\"status\": 0,\"data\": {\"columns\": [{\"name\": \"商品名称\",\"id\": \"name\"},{\"name\": \"交易额\",\"id\": \"amount\"},{\"name\": \"订单数\",\"id\": \"ct\"}],\"rows\": [");

        for (int i = 0; i < productStatsList.size(); i++) {
            ProductStats productStats = productStatsList.get(i);
            if (i >= 1) {
                jsonB.append(",");
            }
            jsonB.append("{\"name\": \"" + productStats.getSpu_name() + "\", \"amount\": " + productStats.getOrder_amount() + ",\"ct\": " + productStats.getOrder_count() + "}");
        }

        jsonB.append("]}}");
        return jsonB.toString();
    }

    @RequestMapping("/category3")
    public Object getProductStatsByCategory3(
        @RequestParam(value = "date", defaultValue = "1") Integer date,
        @RequestParam(value = "limit", defaultValue = "10") Integer limit) {

        if (date == 1) {
            date = now();
        }
        List<ProductStats> productStatsList = productStatsService.getProductStatsByCategory3(date, limit);
        Map resMap = new HashMap();
        resMap.put("status", 0);
        List dataList = new ArrayList();
        for (ProductStats productStats : productStatsList) {
            Map dataMap = new HashMap();
            dataMap.put("name", productStats.getCategory3_name());
            dataMap.put("value", productStats.getOrder_amount());
            dataList.add(dataMap);
        }
        resMap.put("data", dataList);

        return resMap;
    }
    /*@RequestMapping("/category3")
    public String getProductStatsByCategory3(
        @RequestParam(value = "date", defaultValue = "1") Integer date,
        @RequestParam(value = "limit", defaultValue = "10") Integer limit) {

        if (date == 1) {
            date = now();
        }
        List<ProductStats> productStatsList = productStatsService.getProductStatsByCategory3(date, limit);

        StringBuilder jsonB = new StringBuilder("{\"status\": 0,\"data\": [");
        for (int i = 0; i < productStatsList.size(); i++) {
            ProductStats productStats = productStatsList.get(i);
            jsonB.append("{\"name\": \"" + productStats.getCategory3_name() + "\",\"value\": " + productStats.getOrder_amount() + "}");
            if(i < productStatsList.size() - 1){
                jsonB.append(",");
            }
        }
        jsonB.append("]}");
        return jsonB.toString();
    }*/


    @RequestMapping("/tm")
    public String getProductStatsByTm(
        @RequestParam(value = "date", defaultValue = "1") Integer date,
        @RequestParam(value = "limit", defaultValue = "10") Integer limit) {

        if (date == 1) {
            date = now();
        }
        //调用service方法，获取品牌交易额排名
        List<ProductStats> productStatsList = productStatsService.getProductStatsByTm(date, limit);
        //存放所有品牌的集合
        List tmList = new ArrayList();
        //存放品牌对应的交易额的集合
        List amountList = new ArrayList();
        for (ProductStats productStats : productStatsList) {
            tmList.add(productStats.getTrademark_name());
            amountList.add(productStats.getOrder_amount());
        }
        //拼接返回的json字符串

        String json = "{\"status\": 0,\"data\": {" +
            "\"categories\": [\"" + StringUtils.join(tmList, "\",\"") + "\"]," +
            "\"series\": [{\"name\": \"商品品牌\"," +
            "\"data\": [" + StringUtils.join(amountList, ",") + "]" +
            "}]}}";
        return json;
    }

    @RequestMapping("/gmv")
    public String getGmv(@RequestParam(value = "date", defaultValue = "1") Integer date) {
        //如果date参数没有传递，默认是当前日期
        if (date == 1) {
            date = now();
        }
        //调用service层方法，获取总交易额
        BigDecimal gmv = productStatsService.getGmv(date);
        String json = "{\"status\": 0,\"data\": " + gmv + "}";
        return json;
    }

    //获取当前日期
    private Integer now() {
        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return Integer.valueOf(yyyyMMdd);
    }
}
