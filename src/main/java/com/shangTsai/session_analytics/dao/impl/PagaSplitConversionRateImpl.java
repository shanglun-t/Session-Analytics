package com.shangTsai.session_analytics.dao.impl;

import com.shangTsai.session_analytics.dao.IPageSplitConversionRateDAO;
import com.shangTsai.session_analytics.domain.PageSplitConversionRate;
import com.shangTsai.session_analytics.jdbc.JDBCHelper;

/**
 * Implementation of Page Split Conversion Rate DAO
 * @author shangluntsai
 *
 */
public class PagaSplitConversionRateImpl implements IPageSplitConversionRateDAO{

	public void insert(PageSplitConversionRate pageSplitConversionRate) {
		
		String sql = "INSERT INTO " + 
                "page_split_conversion_rate " + 
		         "VALUES" + 
                "(?,?)";

		Object[] params = new Object[] {
				pageSplitConversionRate.getTaskId(),
				pageSplitConversionRate.getConversionRate()};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	
	}
}
