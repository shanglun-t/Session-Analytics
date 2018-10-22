package com.shangTsai.session_analytics.session;

import java.io.Serializable;

import scala.math.Ordered;

/**
 * Sorting key for sorting categories based on clickCount, orderCount, and paymentCount
 * @author shangluntsai
 *
 */
public class CategorySortingKey implements Ordered<CategorySortingKey>, Serializable{

	public CategorySortingKey(long clickCount, long orderCount, long paymentCount) {
		this.clickCount = clickCount;
		this.orderCount = orderCount;
		this.paymentCount = paymentCount;
	}

	private long clickCount; 
	private long orderCount; 
	private long paymentCount; 
	
	@Override
	public boolean $greater(CategorySortingKey other) {
		if(clickCount > other.getClickCount()) {
			return true;
		} else if (clickCount == other.getClickCount() && 
				orderCount > other.getOrderCount()) {
			return true;
		} else if (clickCount == other.getClickCount() && 
				orderCount == other.getOrderCount() &&
				paymentCount > other.getPaymentCount()){
			return true;
		}
		return false;
	}

	@Override
	public boolean $greater$eq(CategorySortingKey other) {
		if($greater(other)) {
			return true;
		} else if (clickCount == other.getClickCount() && 
				orderCount == other.getOrderCount() &&
				paymentCount == other.getPaymentCount()) {
			return true;
		} 
		return false;
	}

	@Override
	public boolean $less(CategorySortingKey other) {
		if(clickCount < other.getClickCount()) {
			return true;
		} else if (clickCount == other.getClickCount() && 
				orderCount < other.getOrderCount()) {
			return true;
		} else if (clickCount == other.getClickCount() && 
				orderCount == other.getOrderCount() &&
				paymentCount < other.getPaymentCount()){
			return true;
		}
		return false;
	}

	@Override
	public boolean $less$eq(CategorySortingKey other) {
		if($less(other)) {
			return true;
		} else if (clickCount == other.getClickCount() && 
				orderCount == other.getOrderCount() &&
				paymentCount == other.getPaymentCount()) {
			return true;
		} 
		return false;
	}

	@Override
	public int compare(CategorySortingKey other) {
		if(clickCount - other.getClickCount() != 0) {
			return (int) (clickCount - other.getClickCount());
		} else if (orderCount - other.getOrderCount() != 0) {
			return (int) (orderCount - other.getOrderCount());
		} else if (paymentCount - other.getPaymentCount() != 0) {
			return (int) (paymentCount - other.getPaymentCount());
		}
		return 0;
	}

	@Override
	public int compareTo(CategorySortingKey other) {
		if(clickCount - other.getClickCount() != 0) {
			return (int) (clickCount - other.getClickCount());
		} else if (orderCount - other.getOrderCount() != 0) {
			return (int) (orderCount - other.getOrderCount());
		} else if (paymentCount - other.getPaymentCount() != 0) {
			return (int) (paymentCount - other.getPaymentCount());
		}
		return 0;
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
