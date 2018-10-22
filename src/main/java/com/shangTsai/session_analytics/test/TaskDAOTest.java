package com.shangTsai.session_analytics.test;

import com.shangTsai.session_analytics.dao.ITaskDAO;
import com.shangTsai.session_analytics.dao.impl.DAOFactory;
import com.shangTsai.session_analytics.domain.Task;

public class TaskDAOTest {

	public static void main(String[] args) throws Exception {
		
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		Task task = taskDAO.findById(2);
		System.out.println(task.getTaskName());
		System.out.println(task.getTaskId());
	}
}
