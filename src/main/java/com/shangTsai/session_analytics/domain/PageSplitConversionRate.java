package com.shangTsai.session_analytics.domain;
/**
 * Page Split Conversion Rate
 * @author shangluntsai
 *
 */
public class PageSplitConversionRate {

	private long taskId;
	private String conversionRate;
	
	public long getTaskId() {
		return taskId;
	}
	public void setTaskId(long taskId) {
		this.taskId = taskId;
	}
	public String getConversionRate() {
		return conversionRate;
	}
	public void setConversionRate(String conversionRate) {
		this.conversionRate = conversionRate;
	}
	
	
}
