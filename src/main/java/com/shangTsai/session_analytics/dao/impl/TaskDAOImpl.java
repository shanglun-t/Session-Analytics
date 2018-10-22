package com.shangTsai.session_analytics.dao.impl;

import java.sql.ResultSet;

import com.shangTsai.session_analytics.dao.ITaskDAO;
import com.shangTsai.session_analytics.domain.Task;
import com.shangTsai.session_analytics.jdbc.JDBCHelper;

/**
 * 
 * Tsak DAO implementation class
 * @author shangluntsai
 *
 */
public class TaskDAOImpl implements ITaskDAO {

	/**
	 * Find Task by taskId
	 * @param taskId
	 * @return 
	 */
	public Task findById(long task_id) {
		
		final Task task  = new Task();
		
		String sql = "SELECT * FROM task WHERE task_id=?";
		Object[] params = new Object[] {task_id};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		
		try {
			jdbcHelper.executeQuery(sql, params, new JDBCHelper.QueryCallBack() {
				
				public void process(ResultSet rs) throws Exception {
					if(rs.next()) {
						long task_id = rs.getLong(1);
						String task_name = rs.getString(2);
						String create_time = rs.getString(3);
						String start_time = rs.getString(4);
						String finish_time = rs.getString(5);
						String task_type = rs.getString(6);
						String task_status = rs.getString(7);
						String task_param = rs.getString(8);
						
						task.setTaskId(task_id);
						task.setTaskName(task_name);
						task.setStartTime(start_time);
						task.setCreateTime(create_time);
						task.setFinishTime(finish_time);
						task.setTaskType(task_type);
						task.setTaskStatus(task_status);
						task.setTaskParam(task_param);
					}
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return task;
	}
}
