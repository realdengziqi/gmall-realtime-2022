package com.atguigu.gmall.realtime.util;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * description:
 * Created by 铁盾 on 2022/3/14
 */
public class DateFormatUtil {
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter dtfFull = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static Long toTs(String dtStr, boolean isFull) {
        LocalDateTime localDateTime = null;
        if(isFull){
            localDateTime = LocalDateTime.parse(dtStr, dtfFull);
        } else {
            localDateTime = LocalDateTime.parse(dtStr, dtf);
        }
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
//        return localDateTime.toInstant(ZoneOffset.of("Z")).toEpochMilli();
    }

    public static Long toTs(String dtStr) {
        return toTs(dtStr, false);
//        return localDateTime.toInstant(ZoneOffset.of("Z")).toEpochMilli();
    }

    public static String toDate(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
//        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.of("Z"));
        return dtf.format(localDateTime);
    }

    public static String toYmdHms(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
//        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.of("Z"));
        return dtfFull.format(localDateTime);
    }

    public static void main(String[] args) {
        System.out.println(toYmdHms(System.currentTimeMillis()));
    }
}
