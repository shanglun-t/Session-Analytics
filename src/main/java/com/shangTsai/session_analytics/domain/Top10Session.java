package com.shangTsai.session_analytics.domain;
/**
 * Top 10 most active session on each category
 * session activities measure by clickClounts
 * @author shangluntsai
 *
 */
public class Top10Session {

	private long taskId;
	private long categoryId;
	private String sessionId;
	private long clickCount;
	
	public long getTaskId() {
		return taskId;
	}
	public void setTaskId(long taskId) {
		this.taskId = taskId;
	}
	public long getCategoryId() {
		return categoryId;
	}
	public void setCategoryId(long categoryId) {
		this.categoryId = categoryId;
	}
	public String getSessionId() {
		return sessionId;
	}
	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}
	public long getClickCount() {
		return clickCount;
	}
	public void setClickCount(long clickCount) {
		this.clickCount = clickCount;
	}
	
}
