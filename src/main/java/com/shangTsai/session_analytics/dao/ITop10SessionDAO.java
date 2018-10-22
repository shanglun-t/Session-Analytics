package com.shangTsai.session_analytics.dao;

import com.shangTsai.session_analytics.domain.Top10Session;

/**
 * Interface of top 10 most active session on each category DAO
 * @author shangluntsai
 *
 */
public interface ITop10SessionDAO {
	
	void insert(Top10Session top10Session);
}
