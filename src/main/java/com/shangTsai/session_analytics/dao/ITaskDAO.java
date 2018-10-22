package com.shangTsai.session_analytics.dao;

import com.shangTsai.session_analytics.domain.Task;

/**
 * 
 * Task DAO Interface
 * @author shangluntsai
 *
 */
public interface ITaskDAO {

	Task findById(long task_id) throws Exception;
	
}
