package com.shangTsai.session_analytics.utils;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.shangTsai.session_analytics.config.ConfigurationManager;
import com.shangTsai.session_analytics.constants.Constants;

/**
 * 
 * @author shangluntsai
 *
 */
public class ParamUtils {

	/**
	 * Obtain task_id from command line arguments
	 * @param args
	 * @return 
	 */
	private static JsonParser jsonParser = new JsonParser();
	
	public static Long  getTaskIdFromArgs (String[] args, String taskType) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		
		if(local) {
			return ConfigurationManager.getLong(taskType);
		} else {
			try {
				if(args != null && args.length > 0) {
					return Long.valueOf(args[0]);
				}
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	

	/**
	 * Obtain task_id from json objects
	 * @param taskParam
	 * @param field
	 * @return 
	 */
	public static String getParamFromJson(JsonObject jsonObject, String field) {
		
		JsonArray jsonArray = jsonParser.parse(jsonObject.toString()).getAsJsonArray();
		
		if(jsonArray != null && jsonArray.size() > 0) {
		  	return jsonArray.getAsJsonObject().get(field).getAsString();
		}
		return null;
	}

}
