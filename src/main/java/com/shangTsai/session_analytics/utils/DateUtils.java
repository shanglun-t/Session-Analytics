package com.shangTsai.session_analytics.utils;

/**
 * 
 * Tools for formating date and time
 * @author shangluntsai
 * 
 */

import java.util.Calendar;
import java.util.Date;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class DateUtils {

	public static final SimpleDateFormat TIME_FORMAT = 
			new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static final SimpleDateFormat DATE_FORMAT = 
			new SimpleDateFormat("yyyy-MM-dd");
	
	/**
	 * Checking if time1 is prior to time2
	 * @param time1
	 * @param time2
	 * @return 
	 */
	public static boolean before(String time1, String time2) {
		try {
			Date datetime1 = TIME_FORMAT.parse(time1);
			Date datetime2 = TIME_FORMAT.parse(time2);
			
			if(datetime1.before(datetime2)) {
				return true;
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return false;
		
	}
	

	/**
	 * Checking if time1 is subsequent to time2
	 * @param time1
	 * @param time2
	 * @return 
	 */
	public static boolean after(String time1, String time2) {
		try {
			Date datetime1 = TIME_FORMAT.parse(time1);
			Date datetime2 = TIME_FORMAT.parse(time2);
			
			if(datetime1.after(datetime2)) {
				return true;
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return false;
		
	}
	

	/**
	 * Calculating time difference in seconds
	 * @param time1
	 * @param time2
	 * @return 
	 */
	public static int minus(String time1, String time2) {
		try {
			Date datetime1 = TIME_FORMAT.parse(time1);
			Date datetime2 = TIME_FORMAT.parse(time2);
			
			long milliSecond = datetime1.getTime() - datetime2.getTime();
			
			return Integer.valueOf(String.valueOf(milliSecond / 1000));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return 0;
	}
	

	/**
	 * Obtain date and hour in (yyyy-MM-dd HH:mm:ss) format
	 * @param dateTime
	 * @return (yyyy-MM-dd_HH) format
	 */
	public static String getDateHour(String dateTime) {
		String date = dateTime.split(" ")[0];  
		String hourMinuteSecond = dateTime.split(" ")[1];
		String hour = hourMinuteSecond.split(":")[0];
		return date + "_" + hour;
	}
	

	/**
	 * Get the date of today
	 * @return 
	 */
	public static String getTodayDate() {
		return DATE_FORMAT.format(new Date());
	}
	

	/**
	 * Get the date of yesterday
	 * @return 
	 */
	public static String getYesterdayDate() {
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		cal.add(Calendar.DAY_OF_YEAR, -1);
		
		Date date = cal.getTime();
		
		return DATE_FORMAT.format(date);
	}
	

	/**
	 * Formating date (yyyy-MM-dd)
	 * @param date
	 * @return 
	 */
	public static String formatDate(Date date) {
		return DATE_FORMAT.format(date);
	}
	
	
	/**
	 * Formating time (yyyy-MM-dd HH:mm:ss)
	 * @param date
	 * @return 
	 */
	public static String formatTime(Date date) {
		return TIME_FORMAT.format(date);
	}
	
	/**
	 * Parse time string
	 * @param time
	 * @return
	 */
	public static Date parseTime(String time) {
		try {
			return TIME_FORMAT.parse(time);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
}
