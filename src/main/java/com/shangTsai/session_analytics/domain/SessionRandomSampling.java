package com.shangTsai.session_analytics.domain;

/**
 * 随机抽取的session
 * @author shangluntsai
 *
 */
public class SessionRandomSampling {
	
	private long task_id;
	private String session_id;
	private String startTime;
	private String searchKeywords;
	private String clickCategoryIds;
	
	public long getTaskId() {
		return task_id;
	}
	public void setTaskid(long task_id) {
		this.task_id = task_id;
	}
	public String getSessionid() {
		return session_id;
	}
	public void setSessionid(String session_id) {
		this.session_id = session_id;
	}
	public String getStartTime() {
		return startTime;
	}
	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}
	public String getSearchKeywords() {
		return searchKeywords;
	}
	public void setSearchKeywords(String searchKeywords) {
		this.searchKeywords = searchKeywords;
	}
	public String getClickCategoryIds() {
		return clickCategoryIds;
	}
	public void setClickCategoryIds(String clickCategoryIds) {
		this.clickCategoryIds = clickCategoryIds;
	}

}
