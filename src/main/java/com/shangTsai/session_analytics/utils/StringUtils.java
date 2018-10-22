package com.shangTsai.session_analytics.utils;
/**
 *
 * String tools
 * @author shangluntsai
 *
 */

public class StringUtils {

	/**
	 * Check if a string is empty
	 * @param string
	 * @return 
	 */
	public static boolean isEmpty(String string) {
		return string == null || "".equals(string);
	}
	

	/**
	 * Check if a string is not empty
	 * @param string
	 * @return 
	 */
	public static boolean isNotEmpty(String string) {
		return string != null || !"".equals(string);
	}
	

	/**
	 * Trim commas on both ends of a string
	 * @param string
	 * @return 
	 */ 
	public static String trimComma(String string) {
		if(string.startsWith(",")) {
			string = string.substring(1);
		}
		if(string.endsWith(",")) {
			string = string.substring(0, string.length() - 1);
		}
		return string;
	}
	

	/**
	 * fill with zero when length of string less then 2 
	 * @param string
	 * @return 
	 */
	public static String fillZero(String string) {
		if(string.length() == 2) {
			return string;
		}else {
			return "0" + string;
		}
	}
	

	/**
	 * Extract field Value from concatenated string
	 * @param string
	 * @param delimiter
	 * @param field
	 * @return 
	 */
	public static String getFieldFromConcatString(
			String string, 
			String delimiter, 
			String field) {
		
		try {
			String[] fields = string.split(delimiter);
			for(String one_field : fields) {
				if(one_field.split(":").length == 2) {
					String fieldName = one_field.split(":")[0];
					String fieldValue = one_field.split(":")[1];
					if(fieldName.equals(field)) {
						return fieldValue;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	
	/**
	 * Set fieldValue from concatenated string
	 * @param string
	 * @param delimiter
	 * @param field
	 * @param newFieldValue
	 * @return 
	 */
	public static String setFieldInConcatString(
			String string, 
			String delimiter, 
			String field, 
			String newFieldValue) {
		
		String[] fields = string.split(delimiter);
		
		for(int i = 0; i < fields.length; ++i) {
			String fieldName = fields[i].split(":")[0]; 
			if(fieldName.equals(field)) {           
				String concatField = fieldName + ":" + newFieldValue;  
				fields[i] = concatField;            
				break;
			}
		}
		
		StringBuffer buffer = new StringBuffer("");
		
		for(int i = 0; i < fields.length; ++i) {
			buffer.append(fields[i]);
			if(i < fields.length - 1) {
				buffer.append("|");
			}
		}
		
		return buffer.toString();
	}
}
