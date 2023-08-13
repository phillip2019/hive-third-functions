package com.chinagoods.bigdata.functions.utils;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 日期帮助类
 *
 * @author xiaowei.song
 */
public class DateUtil {
    public static final Logger logger = LoggerFactory.getLogger(DateUtil.class);

    public static final String DEFAULT_UTC_DATE_TIME = "1970-01-01 00:00:00";

    public static final String DEFAULT_DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String US_DATE_TIME_FORMAT = "MM/dd/yyyy HH:mm:ss";
    //09/Nov/2020:16:11:31 +0800
    public static final String GMT_DATE_TIME_FORMAT = "dd/MMM/yyyy:HH:mm:ss Z";
    /**
     * 2022-02-11T09:12:15+08:00
     **/
    public static final String UTC_DATE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ssXXX";
    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
    public static final String DATE_DS_FORMAT = "yyyyMMdd";

    public static final String US_DATE_FORMAT = "MM/dd/yyyy";

    public static final String TIME_FORMAT = "HH:mm:ss";

    public static final Pattern US_DATE_PATTERN = Pattern.compile("[0-1][0-9]/[0-3][0-9]/20[0-9][0-9]");

    public static final Pattern TIME_PATTERN = Pattern.compile("[0-2][0-9]:[0-5][0-9]:[0-5][0-9]");

    public static final long[] TIME_RATION = {1L, 60L, 3600L};

    public static final DateTimeFormatter DEFAULT_DTF = DateTimeFormatter.ofPattern(DEFAULT_DATE_TIME_FORMAT);
    public static final DateTimeFormatter DEFAULT_DATE_DTF = DateTimeFormatter.ofPattern(DEFAULT_DATE_FORMAT);
    public static final DateTimeFormatter DATE_DS_DTF = DateTimeFormatter.ofPattern(DATE_DS_FORMAT);
    public static final DateTimeFormatter US_DATE_DTF = DateTimeFormatter.ofPattern(US_DATE_FORMAT);
    public static final DateTimeFormatter GMT_DATE_DTF = DateTimeFormatter.ofPattern(GMT_DATE_TIME_FORMAT, Locale.ENGLISH);
    public static final DateTimeFormatter UTC_DATE_TIME_DTF = DateTimeFormatter.ofPattern(UTC_DATE_TIME_FORMAT, Locale.CHINA);
    public static final DateTimeFormatter US_DATE_TIME_DTF = DateTimeFormatter.ofPattern(US_DATE_TIME_FORMAT);
    public static final DateTimeFormatter DEFAULT_TIME_DTF = DateTimeFormatter.ofPattern(TIME_FORMAT);
    public static final Map<String, DateTimeFormatter> FORMATTER_DTF_MAP = ImmutableMap.<String, DateTimeFormatter>builder()
            .put(DEFAULT_DATE_TIME_FORMAT, DEFAULT_DTF)
            .put(DEFAULT_DATE_FORMAT, DEFAULT_DATE_DTF)
            .put(US_DATE_FORMAT, US_DATE_DTF)
            .put(GMT_DATE_TIME_FORMAT, GMT_DATE_DTF)
            .put(US_DATE_TIME_FORMAT, US_DATE_TIME_DTF)
            .put(TIME_FORMAT, DEFAULT_TIME_DTF)
            .put(UTC_DATE_TIME_FORMAT, UTC_DATE_TIME_DTF)
            .put(DATE_DS_FORMAT, DATE_DS_DTF)
            .build();

    public static final ZoneOffset DEFAULT_ZONE_OFFSET = ZoneOffset.of("+8");

    /**
     * 给定日期时间格式，返回班次信息
     *
     * @param dateTimeStr 日期时间字符串
     * @return shift 班次
     **/
    public static String calcShift(String dateTimeStr) {
        LocalDateTime dateTime = LocalDateTime.parse(dateTimeStr, DEFAULT_DTF);
        return calcShift(dateTime, false);
    }

    /**
     * 给定日期时间格式，返回班次信息
     *
     * @param dateTimeStr 日期时间字符串
     * @return shift 班次
     **/
    public static String calcShift(String dateTimeStr, boolean dayDate) {
        LocalDateTime dateTime = LocalDateTime.parse(dateTimeStr, DEFAULT_DTF);
        return calcShift(dateTime, dayDate);
    }

    /**
     * 给定日期时间格式，返回班次信息
     *
     * @param dateTime 日期时间
     * @param dayDate  是否是班次时间，若是，则不减8,反之，减8
     * @return shift 班次
     **/
    public static String calcShift(LocalDateTime dateTime, boolean dayDate) {
        if (!dayDate) {
            dateTime = dateTime.plusHours(-8);
        }
        String shift;
        String dateStr = DEFAULT_DATE_DTF.format(dateTime);
        if (dateTime.getHour() < 12) {
            shift = String.format("%s-D", dateStr);
        } else {
            shift = String.format("%s-N", dateStr);
        }
        return shift;
    }

    /**
     * 给定日期时间格式，返回day date信息
     *
     * @param dateTimeStr 日期时间字符串
     * @return day date 工作日期
     **/
    public static String calcDayDate(String dateTimeStr) {
        LocalDateTime dateTime = LocalDateTime.parse(dateTimeStr, DEFAULT_DTF);
        dateTime = calcDayDate(dateTime);
        return DEFAULT_DATE_DTF.format(dateTime);
    }

    /**
     * 给定日期时间格式，返回day date信息
     *
     * @param dateTimeStr 日期时间字符串
     * @return LocalDateTime dayDate 工作日期时间格式
     **/
    public static LocalDateTime calcDayDate2DateTime(String dateTimeStr) {
        LocalDateTime dateTime = LocalDateTime.parse(dateTimeStr, DEFAULT_DTF);
        dateTime = calcDayDate(dateTime);
        return dateTime;
    }

    public static LocalDateTime calcDayDate(LocalDateTime dateTime) {
        return dateTime.plusHours(-8);
    }

    /**
     * 将日期时间格式字符串转换成日期对象
     *
     * @param dateTimeStr 日期时间字符串
     * @return date 日期实例
     **/
    public static LocalDateTime parse(String dateTimeStr) {
        return parse(dateTimeStr, DEFAULT_DATE_TIME_FORMAT);
    }

    /**
     * 将日期格式字符串按照指定格式转换成日期对象
     *
     * @param dateTimeStr 日期时间字符串
     * @param format      日期格式
     * @return date 日期实例
     **/
    public static LocalDateTime parse(final String dateTimeStr, final String format) {
        DateTimeFormatter formatter = null;
        if (FORMATTER_DTF_MAP.containsKey(format)) {
            formatter = FORMATTER_DTF_MAP.get(format);
        } else {
            formatter = DateTimeFormatter.ofPattern(format);
        }
        return LocalDateTime.parse(dateTimeStr, formatter);
    }

    /**
     * 将日期格式化成字符串
     *
     * @param dateTime 日期时间
     * @param format   日期格式
     * @return dateTimeStr String
     **/
    public static String format(LocalDateTime dateTime, String format) {
        DateTimeFormatter formatter = null;
        if (FORMATTER_DTF_MAP.containsKey(format)) {
            formatter = FORMATTER_DTF_MAP.get(format);
        } else {
            formatter = DateTimeFormatter.ofPattern(format);
        }
        return dateTime.format(formatter);
    }

    /**
     * 将日期格式化成字符串
     *
     * @param dateTime 日期时间
     * @return dateTimeStr String
     **/
    public static String format(LocalDateTime dateTime) {
        return format(dateTime, DEFAULT_DATE_TIME_FORMAT);
    }

    /**
     * 从字符串中提取日期字符串
     * 08/20/2020->08/20/2020
     * 若数据错误，则忽略，填写默认值
     **/
    public static String getDateStr(String dateStr) {
        Matcher matcher = US_DATE_PATTERN.matcher(dateStr);
        matcher.reset();
        if (matcher.find()) {
            return matcher.group(0);
        }
        return format(LocalDateTime.now(), US_DATE_FORMAT);
    }

    /**
     * 从字符串中提取日期字符串
     * 08/20/2020->08/20/2020
     * 若数据错误，则忽略，填写默认值
     **/
    public static String getTimeStr(String str) {
        Matcher matcher = TIME_PATTERN.matcher(str);
        matcher.reset();
        if (matcher.find()) {
            return matcher.group(0);
        }
        return format(LocalDateTime.now(), TIME_FORMAT);
    }

    /**
     * 13位时间戳转换成ds字符串
     **/
    public static String getDate2DSString(long timestamp13) {
        Instant instant = Instant.ofEpochMilli(timestamp13);
        return format(LocalDateTime.ofInstant(instant, DEFAULT_ZONE_OFFSET), DATE_DS_FORMAT);
    }


    public static void main(String[] args) {
//        String dateTime = "2020-08-27 19:58:00";
//        String usDateTime = "08/27/2020 22:58:00";
//        System.out.println(parse(dateTime));
//        System.out.println(format(parse(dateTime)));
//
//        System.out.println(parse(usDateTime, US_DATE_TIME_FORMAT));
//
//        System.out.println(calcDayDate(dateTime));
//
//        System.out.println(calcShift(dateTime));

//        String dateStr = "01/Nov/2020:13:57:29 +0800";
//        System.out.println(parse(dateStr, GMT_DATE_TIME_FORMAT));
        System.out.println(DateUtil.format(DateUtil.parse("2022-02-11T09:12:15+08:00", UTC_DATE_TIME_FORMAT)));
    }
}
