package com.shangTsai.session_analytics.dao;

import com.shangTsai.session_analytics.domain.SessionRandomSampling;

/**
 * session random sampling DAO interface
 * @author shangluntsai
 *
 */
public interface ISessionRandomSamplingDAO {
	
	/**
	 * Insert random sampled session
	 */
	void insert(SessionRandomSampling sessionRandomSampling);
	
}
