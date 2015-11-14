package com.corp.tsdb.utils;

import java.util.TimeZone;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class MyTime {
	private static DateTimeFormatter sdf;
	private static DateTimeFormatter inSdf;

	static {
		sdf = DateTimeFormat.forPattern("yyyyMMddHHmmssSSS").withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("GMT+8")));
		inSdf = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("GMT+8")));
	}

	/**
	 * transform format long Object to string(yyyyMMddHHmmssSSS)
	 * 
	 * @param timeL
	 * @return
	 */
	public static String Long2FormatString(Long timeL) {
		DateTime dt = new DateTime(timeL);		
		return dt.toString(sdf);
	}

	/**
	 * transform format string(yyyyMMddHHmmssSSS) to long Object
	 * 
	 * @param timeString
	 * @return
	 */
	public static Long FormatString2Long(String timeString) {
		DateTime date = DateTime.parse(timeString, sdf);
		return date.getMillis();
	}

	/**
	 * transform format string(yyyy-MM-dd HH:mm:ss:SS.S) to long Object
	 * 
	 * @param timeString
	 * @return
	 */
	public static Long formatInString2Long(String timeString) {
		DateTime date = DateTime.parse(timeString,inSdf);
		return date.getMillis();
	}

	/**
	 * transform format long Object to string(yyyy-MM-dd HH:mm:ss:SS.S)
	 * 
	 * @param timeLong
	 * @return
	 */
	public static String formatLong2InString(long timeLong) {
		DateTime dt = new DateTime(timeLong);
		return dt.toString(inSdf);
	}
	
	public static void main(String[] args) {
		String date = "2012-05-01 01:00:01.499";
		long dateLong = formatInString2Long(date);
		System.out.println(dateLong);
		date = formatLong2InString(dateLong);
		System.out.println(date);
		System.out.println(Long2FormatString(dateLong));
		System.out.println(formatLong2InString(dateLong));
	}
}
