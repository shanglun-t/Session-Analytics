package com.shangTsai.session_analytics.utils;
/**
 * 
 * Verifying tools
 * @author shangluntsai
 *
 */
public class ValidUtils {

	/**
	 * Verifying if value of paramField can be located between certain range of field
	 * @param data
	 * @param dataField
	 * @param parameter
	 * @param startParamField
	 * @param endParamField
	 * @return  
	 */
	public static boolean between(String data, String dataField, 
			String parameter, String startParamValue, String endParamValue) {
		
		String startParamFieldString = StringUtils.getFieldFromConcatString(
				parameter, "\\|", startParamValue);
		String endParamFieldString = StringUtils.getFieldFromConcatString(
				parameter, "\\|", endParamValue);
		
		if(startParamFieldString == null || endParamFieldString == null) {
			return true;
		}
		
		int startParamFieldValue = Integer.valueOf(startParamFieldString);
		int endParamFieldValue = Integer.valueOf(endParamFieldString);
		
		String dataFieldString = StringUtils.getFieldFromConcatString(data, "\\|", dataField);
		
		if(dataFieldString != null) {
			int dataFieldValue = Integer.valueOf(dataFieldString);
			if(dataFieldValue >= startParamFieldValue && 
					dataFieldValue <= endParamFieldValue) {
				return true;
			}else {
				return false;
			}
		}
		
		return false;
	}
	
	
	/**
	 * Verifying if value of paramField can be located in dataField
	 * @param data
	 * @param dataField
	 * @param parameter
	 * @param paramField
	 * @return 
	 */
	public static boolean in(String data, String dataField, 
			String parameter, String paramField) {
		
		String paramFieldValue = StringUtils.getFieldFromConcatString(
				parameter, "\\|", paramField);
		
		if(paramFieldValue == null) {
			return true;
		}
		
		String[] paramFieldValueSplited = paramFieldValue.split(",");
		
		String dataFieldValue = StringUtils.getFieldFromConcatString(
				data, "\\|", dataField);
		
		if(dataFieldValue != null) {
			String[] dataFieldValueSplited = dataFieldValue.split(",");
			
			for(String singleDataFieldValue : dataFieldValueSplited) {
				for(String singleParamFieldValue : paramFieldValueSplited) {
					if(singleDataFieldValue.equals(singleParamFieldValue)) {
						return true;
					}
				}
			}
			
		}
		return false;
	}
	

	/**
	 * Verifying if value of paramField equals to value of dataField
	 * @param data
	 * @param dataField
	 * @param parameter
	 * @param paramField
	 * @return boolean
	 */
	public static boolean equal(String data, String dataField, 
			String parameter, String paramField) {
		
		String paramFieldValue = StringUtils.getFieldFromConcatString(
				parameter, "\\|", paramField);
		
		if(paramFieldValue == null) {
			return true;
		}
		
		String dataFieldValue = StringUtils.getFieldFromConcatString(
				data, "\\|", dataField);
		
		if(dataFieldValue != null) {
			if(dataFieldValue.equals(paramFieldValue)) {
				return true;
			}
		}
		return false;
	}
	
}
