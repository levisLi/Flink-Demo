package com.galaxy.neptune.flink.utils;

import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;


public class DateUtil {

    public final static DateTimeFormatter settleParseDateFormat =  DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public final static DateTimeFormatter settleParseFormat =  DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public final static SimpleDateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd");
    //获取当前年
    public static String YEAR(){
        return settleParseDateFormat.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(Instant.now().toEpochMilli()), ZoneId.systemDefault()));
    }

    //获取下一天
    public static String Day(){
        return LocalDate.now().format(settleParseDateFormat);
    }

    //获取本周日期
    public static String CurrentWeek(String date){
        WeekFields weekFields = WeekFields.of(DayOfWeek.MONDAY,7);
        LocalDate localDate = LocalDate.parse(date, settleParseDateFormat);
        Integer week=localDate.get(weekFields.weekOfYear());
        int dateYear = localDate.getYear();
        return dateYear+"-"+week;
    }

    //获取本周日期
    public static String SubWeek(String date,Integer number){
        WeekFields weekFields = WeekFields.of(DayOfWeek.MONDAY,7);
        LocalDate localDate = LocalDate.parse(date, settleParseDateFormat);
        Integer currentWeek=localDate.get(weekFields.weekOfYear());
        int dateYear;
        Integer subWeek;
        if(currentWeek>number){
             subWeek=currentWeek-number;
             dateYear = localDate.getYear();
        }else{
            subWeek=currentWeek+getWeekNumberOfYear(localDate.getYear())-number;
            dateYear = localDate.getYear()-1;
        }
        return dateYear+"-"+subWeek;
    }

    //日期转换,用于异常日期处理
    public static String dateTransform(String date){
        String time;
        try {
            time=settleParseFormat.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(date)), TimeZone.getDefault().toZoneId()));/*
            time=sdf.format(new Date(Long.parseLong(date)));*/
        }catch (Exception e){
            time="9999-12-31 00:00:00";
        }
        return time;
    }

    //获取指定周数前n周
    public static String subWeekByWeek(String date,Integer number){
       String subWeek= SubWeek(date,number);
       String year=subWeek.split("-")[0];
       String week=subWeek.split("-")[1];
        if(Integer.valueOf(week)<10){
            return year+"-0"+week;
        }else {
            return subWeek;
        }
    }

    public static String getCurrentDate(String keys){
        String dateValue=keys.split("_")[1];
        String year=dateValue.substring(0,4);
        String week=dateValue.substring(5);
        if(Integer.valueOf(week)<10){
            return year+"-0"+week;
        }else {
            return dateValue;
        }
    }


    //获取指定周数前n周
    public static List<String> agoWeekList(String date, Integer number){
        List<String>agoWeek=new ArrayList<>();
        for(int i=0;i<number;i++){
            agoWeek.add( SubWeek(date,i));
        }
        return agoWeek;
    }


    //获取当前年份周数
    public  static  int getWeekNumberOfYear(int year){
        boolean is53weekYear = LocalDate.of(year, 1, 1).getDayOfWeek() == DayOfWeek.THURSDAY ||
                LocalDate.of(year, 12, 31).getDayOfWeek() == DayOfWeek.THURSDAY;
        return is53weekYear ? 53 : 52;
    }

    //获取下一天
    public static String SubDay(String date,Integer number){
        return LocalDate.parse(date,settleParseDateFormat).plusDays(-number).format(settleParseDateFormat);
    }

    //获取下一天
    public static String NextDay(String date){
        return LocalDate.parse(date,settleParseDateFormat).plusDays(1).format(settleParseDateFormat);
    }

    //获取下一天
    public static String NOW(){
        return LocalDateTime.now().format(settleParseFormat);
    }

    //获取下一天
    public static Long NOW_LONG(){
        return LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

}
