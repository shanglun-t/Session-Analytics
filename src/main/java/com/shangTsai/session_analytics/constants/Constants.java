package com.shangTsai.session_analytics.constants;

public interface Constants {

	/**
	 * Configuration related constants
	 */
	String JDBC_DRIVER = "jdbc_driver";
	String JDBC_DATASOURCE_SIZE = "jdbc_datasource_size";
	
	String JDBC_CONN = "jdbc_conn";
	String JDBC_USERNAME = "jdbc_username";
	String JDBC_PASSWORD = "jdbc_password";
	String SPARK_LOCAL = "spark_local";
	String SPARK_LOCAL_TASKID_SESSION = "spark_local.taskId.session";
	String SPARK_LOCAL_TASKID_PAGE = "spark_local.taskId.page";
	
	/**
	 * Spark related constants
	 */
	String APP_USER_SESSION_ANALYTICS = "UserVisitSessionAnalytics";
	String APP_PAGE_CONVERSION_ANALYTICS = "PageConversionAnalytics";
	
	String FIELD_SESSION_ID = "session_id";
	String FIELD_SEARCH_KEYWORDS = "search_keywords";
	String FIELD_CLICK_CATEGORY_IDS = "click_category_ids";
	String FIELD_AGE = "age";
	String FIELD_OCCUPATION = "occupation";
	String FIELD_CITY = "city";
	String FIELD_GENDER = "gender";
	String FIELD_VISIT_LENGTH = "visit_length";
	String FIELD_STEP_LENGTH = "step_length";
	String FIELD_START_TIME = "start_time";
	
	String FIELD_CATEGORY_ID = "category_id";
	String FIELD_CLICK_COUNT = "click_count";
	String FIELD_ORDER_COUNT = "order_count";
	String FIELD_PAYMENT_COUNT = "payment_count";
	
	String SESSION_COUNT = "session_count";
	String TIME_PERIOD_1s_3s = "1s_3s";
	String TIME_PERIOD_4s_6s = "4s_6s";
	String TIME_PERIOD_7s_9s = "7s_9s";
	String TIME_PERIOD_10s_30s = "10s_30s";
	String TIME_PERIOD_30s_60s = "30s_60s";
	String TIME_PERIOD_1m_3m = "1m_3m";
	String TIME_PERIOD_3m_10m = "3m_10m";
	String TIME_PERIOD_10m_30m = "10m_30m";
	String TIME_PERIOD_30m = "30m";
	String STEP_PERIOD_1_3 = "1_3";
	String STEP_PERIOD_4_6 = "4_6";
	String STEP_PERIOD_7_9 = "7_9";
	String STEP_PERIOD_10_30 = "10_30";
	String STEP_PERIOD_30_60 = "30_60"; 
	String STEP_PERIOD_60 = "60";
	
	String PARAM_TARGET_PAGE_FLOW = "target_PageFlow";
	
	/**
	 *  Task related constants
	 */
	String SELECT_START_DATE = "select_start_date";
	String SELECT_END_DATE = "select_end_date";
	String SELECT_MIN_AGE = "min_Age";
	String SELECT_MAX_AGE = "max_Age";
	String SELECT_OCCUPATIONS = "occupations";
	String SELECT_CITIES = "cities";
	String SELECT_GENDER = "gender";
	String SELECT_KEYWORDS = "keywords";
	String SELECT_CATEGORY_IDS = "categoryIds";
	
	
	
}
