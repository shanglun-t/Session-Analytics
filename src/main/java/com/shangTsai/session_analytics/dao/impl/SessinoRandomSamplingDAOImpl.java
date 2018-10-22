package com.shangTsai.session_analytics.dao.impl;

import com.shangTsai.session_analytics.dao.ISessionRandomSamplingDAO;
import com.shangTsai.session_analytics.domain.SessionRandomSampling;
import com.shangTsai.session_analytics.jdbc.JDBCHelper;

/**
 * Implementation of Session Random Sampling DAO
 * @author shangluntsai
 *
 */
public class SessinoRandomSamplingDAOImpl implements ISessionRandomSamplingDAO {

	public void insert(SessionRandomSampling sessionRandomSampling) {
		
		String sql = "INSERT INTO session_random_extract VALUES(?,?,?,?,?)";
		
		Object[] params = new Object[]{
				sessionRandomSampling.getTaskId(),
				sessionRandomSampling.getSessionid(),
				sessionRandomSampling.getStartTime(),
				sessionRandomSampling.getSearchKeywords(),
				sessionRandomSampling.getClickCategoryIds()};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

}
