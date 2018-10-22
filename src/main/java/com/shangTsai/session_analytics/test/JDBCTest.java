package com.shangTsai.session_analytics.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCTest {

	private static final String CONN_STRING = 
			"jdbc:mysql://localhost:3306/explorecalifornia?useSSL=false";
	private static final String USERNAME = "dbuser";
	private static final String PASSWORD = "dbuser";
	
	public static void main (String[] args) throws SQLException {
		insert();
		update();
		delete();
		select();
		prepearedStmt();
	}

	private static void prepearedStmt() {
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String sql = "SELECT * FROM tours WHERE tourId=?";
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(CONN_STRING, USERNAME, PASSWORD);
			stmt = conn.prepareStatement(            // prepared statement
					sql, 
					ResultSet.TYPE_SCROLL_INSENSITIVE, 
					ResultSet.CONCUR_READ_ONLY);
			
			stmt.setInt(1, 20); // set tourId=20
			rs = stmt.executeQuery();
			
			while(rs.next()) {
				System.out.println(rs.getInt("tourId") + " : " + rs.getString("tourName"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			close(conn, stmt, rs);
		}
	}
	
	private static void select() {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String sql = "SELECT * FROM tours WHERE tourId=3";
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(CONN_STRING, USERNAME, PASSWORD);
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			
			while(rs.next()) {
				System.out.println(rs.getInt("tourId") + " : " + rs.getString("tourName"));
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			close(conn, stmt, rs);
		}
		
	}

	private static void delete() {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String sql = "DELETE FROM tours WHERE tourId=89";
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(CONN_STRING, USERNAME, PASSWORD);
			stmt = conn.createStatement();
			int rowsAffected = stmt.executeUpdate(sql);
			
		    System.out.println(rowsAffected + " rows deleted");
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			close(conn, stmt, rs);
		}
		
	}

	private static void update() {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String sql = "UPDATE tours SET price=250.0 WHERE tourId=1";
		
		try {
			//Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(CONN_STRING, USERNAME, PASSWORD);
			System.out.println("Connected!");
			
			stmt = conn.createStatement(
					ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			int rowsAffected = stmt.executeUpdate(sql);
			
		    System.out.println(rowsAffected + " rows updated.");
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			close(conn, stmt, rs);
		}
		
	}

	private static void insert() throws SQLException {
		
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String sql = "INSERT INTO tours " + 
		        "(tourId, packageId, tourName, blurb, description, price, difficulty, graphic, length, region, keywords) " + 
				"VALUES" +
				"(70, 12, 'Alaska_Cruising', 'Alaska', 'Good', 255.00, 'Easy', 'map_alaska.jif', 3, 'Northeast', 'Cruising')";
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(CONN_STRING, USERNAME, PASSWORD);
			stmt = conn.createStatement();
			int rowsAffected = stmt.executeUpdate(sql);
			rs = stmt.executeQuery("select * from tours order by tourId desc");
			System.out.println(rowsAffected + " rows afftected by insertion");
			while(rs.next()) {
				System.out.println(rs.getInt("tourId") + " : " + rs.getString("tourName"));
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			close(conn, stmt, rs);
		}
	}
	
	
	// close Connection
	private static void close(Connection conn, Statement stmt, ResultSet rs) {
		try {
			if (rs != null) {
				rs.close();
			}
			if(stmt != null) {
				stmt.close();
			}
			if(conn != null) {
				conn.close();
			}
		} catch (Exception e2) {
			e2.printStackTrace();
		}
	}
}
