package com.shangTsai.session_analytics.dao.impl;

import com.shangTsai.session_analytics.dao.ITop10CategoryDAO;
import com.shangTsai.session_analytics.domain.Top10Category;
import com.shangTsai.session_analytics.jdbc.JDBCHelper;
/**
 * implementation of top 10 categories DAO 
 * @author shangluntsai
 *
 */
public class Top10CategoryDAOImpl implements ITop10CategoryDAO {

	public void insert(Top10Category category) {
		String sql = "INSERT INTO " + 
                "top10_category " + 
		         "VALUES" + 
                "(?,?,?,?,?)";

		Object[] params = new Object[] {
				category.getTaskId(),
				category.getCategoryId(),
				category.getClickCount(),
				category.getOrderCount(),
				category.getPaymentCount()};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	
	}
}
