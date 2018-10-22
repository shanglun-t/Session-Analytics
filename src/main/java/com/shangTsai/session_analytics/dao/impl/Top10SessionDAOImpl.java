package com.shangTsai.session_analytics.dao.impl;

import com.shangTsai.session_analytics.dao.ITop10SessionDAO;
import com.shangTsai.session_analytics.domain.Top10Session;
import com.shangTsai.session_analytics.jdbc.JDBCHelper;

/**
 * Implementation of top 10 most active session on each category DAO
 * @author shangluntsai
 *
 */
public class Top10SessionDAOImpl implements ITop10SessionDAO{

	public void insert(Top10Session top10Session) {
		
		String sql = "INSERT INTO " + 
                "top10_session " + 
		         "VALUES" + 
                "(?,?,?,?)";

		Object[] params = new Object[] {
				top10Session.getTaskId(),
				top10Session.getCategoryId(),
				top10Session.getSessionId(),
				top10Session.getClickCount()};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
		
	}
}
