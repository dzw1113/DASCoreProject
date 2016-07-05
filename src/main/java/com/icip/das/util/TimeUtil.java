package com.icip.das.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/** 
 * @Description: TODO
 * @author  yzk
 * @date 2016年3月7日 下午5:08:22 
 * @update	
 */
public class TimeUtil {
	
	//定义标准日期格式	
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	
	/** 
	 * 定义时间类型常量 
	 */  
	private final static int SECOND = 1;  
	private final static int MINUTE = 2;  
	private final static int HOUR = 3;  
	private final static int DAY = 4;
	
	/** 
	 * 把一个日期，按照某种格式 格式化输出 
	 * @param date 日期对象 
	 * @return 返回字符串类型 
	 */  
	public static String formatDate(Date date){
	    return sdf.format(date);  
	}  
	
	/** 
	 * 将字符串类型的时间转换为Date类型 
	 * @param str 时间字符串 
	 * @return 返回Date类型 
	 */  
	public static Date formatString(String str){
	    Date time=null;  
	    try {  
	        time = sdf.parse(str);  
	    } catch (ParseException e) {
	        e.printStackTrace();  
	    }  
	    return time;  
	}  
	
	/** 
	 * 将一个表示时间段的数转换为毫秒数 
	 * @param num 时间差数值,支持小数 
	 * @param type 时间类型：1->秒,2->分钟,3->小时,4->天 
	 * @return long类型时间差毫秒数，当为-1时表示参数有错 
	 */  
	public static long formatToTimeMillis(double num, int type) {
	    if (num <= 0)  
	        return 0;  
	    switch (type) {  
	    case SECOND:  
	        return (long) (num * 1000);  
	    case MINUTE:  
	        return (long) (num * 60 * 1000);  
	    case HOUR:  
	        return (long) (num * 60 * 60 * 1000);  
	    case DAY:  
	        return (long) (num * 24 * 60 * 60  * 1000);  
	    default:  
	        return -1;  
	    }  
	} 
	
	/** 
	 * 获取某一指定时间的前一段时间 
	 * @param num  时间差数值 
	 * @param type 时间差类型：1->秒,2->分钟,3->小时,4->天 
	 * @param date 参考时间 
	 * @return 返回格式化时间字符串 
	 */  
	public static String getPreTimeStr(double num,int type, Date date){
	    long nowLong = date.getTime();//将参考日期转换为毫秒时间  
	    Date time = new Date(nowLong - formatToTimeMillis(num, type));//减去时间差毫秒数  
	    return formatDate(time);  
	}  
	
	/** 
	 * 获取某一指定时间的前一段时间 
	 * @param num 时间差数值 
	 * @param type 时间差类型：1->秒,2->分钟,3->小时,4->天 
	 * @param date 参考时间 
	 * @return 返回Date对象 
	 */  
	public static Date getPreTime(double num,int type, Date date){  
	    long nowLong=date.getTime();//将参考日期转换为毫秒时间  
	    Date time = new Date(nowLong-formatToTimeMillis(num, type));//减去时间差毫秒数  
	    return time;  
	}
	
	/** 
	 * 获取某一指定时间的后一段时间 
	 * @param num 时间差数值 
	 * @param type 时间差类型：1->秒,2->分钟,3->小时,4->天 
	 * @param date 参考时间 
	 * @return 返回格式化时间字符串 
	 */  
	public static String getNextTimeStr(double num,int type, Date date){
	    long nowLong = date.getTime();//将参考日期转换为毫秒时间  
	    Date time = new Date(nowLong + formatToTimeMillis(num, type));//加上时间差毫秒数  
	    return formatDate(time);  
	}
	
	/** 
	 * 获取某一指定时间的后一段时间 
	 * @param num 时间差数值 
	 * @param type 时间差类型：1->秒,2->分钟,3->小时,4->天 
	 * @param date 参考时间 
	 * @return 返回Date对象 
	 */  
	public static Date getNextTime(double num,int type, Date date){
	    long nowLong=date.getTime();//将参考日期转换为毫秒时间  
	    Date time = new Date(nowLong + formatToTimeMillis(num, type));//加上时间差毫秒数  
	    return time;  
	}
	
    /** 
     * 获取当前的时间，new Date()获取当前的日期 
     * @return 
     */  
    public static String getNowTime(){  
        return formatDate(new Date());  
    }  
    
    public static void main(String[] args) {
    	double interval = Double.valueOf("600");
    	System.out.println(getNextTime(interval,1,new Date()));
	}
    
}
