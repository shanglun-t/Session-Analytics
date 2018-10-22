package com.shangTsai.session_analytics.utils;

import com.google.gson.JsonObject;
import com.shangTsai.session_analytics.config.ConfigurationManager;
import com.shangTsai.session_analytics.constants.Constants;
import com.shangTsai.session_analytics.test.GenerateMockData;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkUtils {

	/**
	 * Setup Master of SparkConf based on local or production environment 
	 * @param conf
	 */
	public static void setMaster(SparkConf conf) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		
		if(local) {
			conf.setMaster("local[*]");  
		}
	}
	
	
	// -----------------------------------------------------------------------------------------.
	
	
	/**
	 * Generate mock data
	 * @param sc
	 * @param 
	 */
	public static void mockData(
			JavaSparkContext sc, 
			SparkSession session) {
		
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		
		if(local) {
			GenerateMockData.mock(sc, session);
		}	
	}
	
	
	// -----------------------------------------------------------------------------------------.
	
	
	/**
	 * Get RDD of parameters of users visits within a certain range of date
	 * @param session
	 * @param taskParam
	 * @return
	 */
	public static JavaRDD<Row> getActionRDDByDateRange(
			SparkSession session, 
			JsonObject taskParam){
		
		String startDate = ParamUtils.getParamFromJson(taskParam, Constants.SELECT_START_DATE);
		String endDate = ParamUtils.getParamFromJson(taskParam, Constants.SELECT_END_DATE);
		
		String sql = "SELECT * " + 
	             "FROM user_visit_ation " + 
			     "WHERE date >='" + startDate + "' " +
	             "AND date <='" + endDate + "'";
		
		Dataset<Row> actionDF = session.sql(sql);
		
	//	return actionDF.javaRDD().repartition(1000);   
		
		return actionDF.javaRDD();
	}

}
