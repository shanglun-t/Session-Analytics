package com.shangTsai.session_analytics.dao.impl;

import com.shangTsai.session_analytics.dao.ISessionDetailsDAO;
import com.shangTsai.session_analytics.domain.SessionDetails;
import com.shangTsai.session_analytics.jdbc.JDBCHelper;

/**
 * Session details implementation
 * @author shangluntsai
 *
 */
public class SessionDetailsDAOImpl implements ISessionDetailsDAO {

	public void insert(SessionDetails sessionDetails) {
		
		String sql = "INSERT INTO " + 
	                 "session_detail " + 
			         "VALUES" + 
	                 "(?,?,?,?,?,?,?,?,?,?,?,?)";
	
		Object[] params = new Object[] {
				sessionDetails.getTaskId(),
				sessionDetails.getUserId(),
				sessionDetails.getSessionId(),
				sessionDetails.getPageId(),
				sessionDetails.getActionTime(),
				sessionDetails.getSearchKeyword(),
				sessionDetails.getClickCategoryId(),
				sessionDetails.getClickProductId(),
				sessionDetails.getOrderCategoryIds(),
				sessionDetails.getOrderProductIds(),
				sessionDetails.getPayCategoryIds(),
				sessionDetails.getPayProductIds()};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
		
	}
}
