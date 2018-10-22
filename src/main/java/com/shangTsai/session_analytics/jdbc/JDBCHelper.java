package com.shangTsai.session_analytics.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.shangTsai.session_analytics.config.ConfigurationManager;
import com.shangTsai.session_analytics.constants.Constants;

import java.util.LinkedList;
import java.util.List;

public class JDBCHelper {

	/* 
	 * STEP 1: load mysql driver in a static block
	 */
	static {
		try {
			String driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	
	/*  
	 * STEP 2: Instantiate JDBCManager's singleton
	 */
	private static JDBCHelper instance = null;
	
	public static JDBCHelper getInstance() {
		if(instance == null) {
			synchronized(JDBCHelper.class) {
				if(instance == null) {
					instance = new JDBCHelper();
				}
			}
		}
		return instance;
	}
	
	
	/* 
	 * STEP 3: instantiate datasource 
	 */
	private LinkedList<Connection> dataSource = new LinkedList<Connection>();
	
	private JDBCHelper() {
		int dataSourceSize = ConfigurationManager.getInteger(
				Constants.JDBC_DATASOURCE_SIZE);
		
		for(int i = 0; i < dataSourceSize; ++i) {
			String url = ConfigurationManager.getProperty(Constants.JDBC_CONN);
			String username = ConfigurationManager.getProperty(Constants.JDBC_USERNAME);
			String password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
			
			try {
				Connection conn = DriverManager.getConnection(url, username, password);
				dataSource.push(conn);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	/*
	 * STEP 4: method of Connection to datasource
	 */
	public synchronized Connection getConnection() {
		while(dataSource.size() == 0) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return dataSource.poll();
	}
	
	
	/*
	 * STEP 5: CRUD methods
	 */
	
	
	/*
	 * Execute query
	 * @param sql
	 * @param query
	 * @return number of rows affected
	 */
	public int executeUpdate(String sql, Object[] params) {
		Connection conn = null;
		PreparedStatement stmt = null;
		int rowsAffected = 0;
		
		try {
			conn = getConnection();
			stmt = conn.prepareStatement(sql);
			
			for(int i = 0; i < params.length; ++i) {
				stmt.setObject(i + 1, params[i]);
			}
			rowsAffected = stmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if(conn != null) {
				dataSource.push(conn);  // return Connection to dataSource 
			}
		}
		
		return rowsAffected;
	}
	
	
	/*
	 * Execute batch query
	 * @param sql
	 * @param params
	 * @param callBack
	 */
	public void executeQuery(String sql, Object[] params, 
			QueryCallBack callBack) throws Exception {
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		
		try {
			conn = getConnection();
			stmt = conn.prepareStatement(sql);
			
			for(int i = 0; i < params.length; ++i) {
				stmt.setObject(i + 1, params[i]);
			}
			
			rs = stmt.executeQuery();
			callBack.process(rs);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * Execute batch query
	 * @param sql
	 * @param batch queries
	 * @return number of rows affected
	 */
	public int[] executeBatchQuery(String sql, List<Object[]> paramsList) {
		
		int[] result = null;
		Connection conn = null;
		PreparedStatement stmt = null;
		
		try {
			conn = getConnection();
			
			// step1: disable auto-summit for batch querying 
			conn.setAutoCommit(false);  
			
			stmt = conn.prepareStatement(sql);
			
			// step2: introduce each parameter-set into each corresponding query
			for(Object[] params : paramsList) {
				for(int i = 0; i < params.length; ++i) {
					stmt.setObject(i + 1, params[i]);
				}
				stmt.addBatch();
			}
			
			// step3: Statement execute batch query
			result = stmt.executeBatch();
			 
			// step4: Connection instance summit queries
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	/* 
	 * callback interface
	 */
	public static interface QueryCallBack{
		void process(ResultSet rs) throws Exception;
	}
}
