package com.shangTsai.session_analytics.dao;

import com.shangTsai.session_analytics.domain.SessionDetails;

/**
 * session details interface
 * @author shangluntsai
 *
 */
public interface ISessionDetailsDAO {
	
	/**
	 * insert session details
	 * @param sessionDetail
	 */
	void insert(SessionDetails sessionDetail);

}
