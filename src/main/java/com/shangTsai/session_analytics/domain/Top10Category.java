package com.shangTsai.session_analytics.domain;

public class Top10Category {

	private long taskId;
	private long categoryId;
	private long clickCount;
	private long orderCount;
	private long paymentCount;
	
	public long getTaskId() {
		return taskId;
	}
	public void setTaskId(long taskId) {
		this.taskId = taskId;
	}
	public long getCategoryId() {
		return categoryId;
	}
	public void setCategoryId(long categotyId) {
		this.categoryId = categotyId;
	}
	public long getClickCount() {
		return clickCount;
	}
	public void setClickCount(long clickCount) {
		this.clickCount = clickCount;
	}
	public long getOrderCount() {
		return orderCount;
	}
	public void setOrderCount(long orderCount) {
		this.orderCount = orderCount;
	}
	public long getPaymentCount() {
		return paymentCount;
	}
	public void setPaymentCount(long paymentCount) {
		this.paymentCount = paymentCount;
	}
	
	
}
