package com.shangTsai.session_analytics.config;

import java.util.Properties;

import java.io.InputStream;

/**
 * 
 * Configuration Manager
 * @author shangluntsai
 *
 */
public class ConfigurationManager {

	private static Properties property = new Properties();
	
	static {
		try {
			InputStream inStream = ConfigurationManager.class.getClassLoader()
					.getResourceAsStream("config.properties");
			property.load(inStream);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * Get corresponding value of a  certain key
	 * @param sparkLocal
	 * @return
	 */
	public static String getProperty ( String key ) {
		return property.getProperty(key);
	}
	
	
	/**
	 * Obtain Integer properties
	 * @param key
	 * @return
	 */
	public static Integer getInteger(String key) {
		String value = getProperty(key);
		try {
			return Integer.valueOf(value);
		} catch (NumberFormatException e) {
			e.printStackTrace();
			return 0;
		}
	}
	
	

	/**
	 * Obtain Boolean value
	 * @param key
	 * @return
	 */
	public static Boolean getBoolean(String key) {
		String value = getProperty(key);
		
		try {
			return Boolean.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return false;
	}
	
	
	
	/**
	 * Obtain Long value
	 * @param key
	 * @return
	 */
	public static Long getLong(String key) {
		String value = getProperty(key);
		
		try {
			return Long.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return 0L;
	}
}
