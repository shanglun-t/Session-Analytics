package com.shangTsai.session_analytics.test;

import com.shangTsai.session_analytics.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

public class JDBCManagerTest {

	public static void main(String[] args) throws Exception {
		// Get a JDBCManager singleton
		JDBCHelper jdbcManager = JDBCHelper.getInstance();
		
		// TEST1: execution of update -- tested OK
//		String sql = "INSERT INTO employees " + 
//		        "(id, last_name, first_name, email, department, salary) " + 
//				"VALUES " +
//				"(?, ?, ?, ?, ?, ?)";
//		
//		jdbcManager.executeUpdate(
//				sql, 
//				new Object[]{34, "Wright", "Eric", "eric.wright@foo.com", "HR", 43000.00});
		
		// TEST2: execution of query -- tested OK
//		final Map<String, Object> testUser = new HashMap<String, Object>();
//		String sql2 = "SELECT last_name, salary FROM employees WHERE id=?";	
//		jdbcManager.executeQuery(sql2, 
//				new Object[] {10}, 
//				new JDBCManager.QueryCallBack() {
//					public void process(ResultSet rs) throws Exception {
//						if(rs.next()) {
//							String lastName = rs.getString(1);
//							Double salary = rs.getDouble(2);
//							testUser.put("last_name", lastName);
//							testUser.put("salary", salary);
//						}
//						
//					}
//				});   // An internal interface to QueryCallBack()
//		System.out.println(testUser.get("last_name") + " : " + testUser.get("salary"));
		
		
		// TEST3: execution of batch query -- tested OK
		String sql3 = "INSERT INTO employees " + 
		        "(id, last_name, first_name, email, department, salary) " + 
				"VALUES " +
				"(?, ?, ?, ?, ?, ?)";
		List<Object[]> paramsList = new ArrayList<Object[]>();
		paramsList.add(new Object[]{38, "Franklin", "Ben", "ben.franklin@foo.com", "Engineering", 93000.00});
		paramsList.add(new Object[]{39, "Coopper", "Jack", "jack.coopper@foo.com", "Engineering", 83000.00});
		
		jdbcManager.executeBatchQuery(sql3, paramsList);
	}
	
}
