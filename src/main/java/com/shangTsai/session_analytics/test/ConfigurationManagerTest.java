package com.shangTsai.session_analytics.test;

import com.shangTsai.session_analytics.config.ConfigurationManager;
import com.shangTsai.session_analytics.constants.Constants;

/**
 * 
 * Configuration Components Testing
 * @author shangluntsai
 *
 */
public class ConfigurationManagerTest {

	public static void main(String[] args) {
		String testKey1 = ConfigurationManager.getProperty(Constants.SPARK_LOCAL_TASKID_SESSION);
		String testKey2 = ConfigurationManager.getProperty(Constants.SPARK_LOCAL_TASKID_PAGE);
		System.out.println(testKey1);
		System.out.println(testKey2);
	}
	
}
