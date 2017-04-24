package com.vitessedata.phi.deepgreen;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.workflow.CredentialsProvider;


/*
 * Quick utilities to access deepgreen.   Most of them are just for saving some typing.
 */  


public class DGUtil {
	public static Connection rawConn(DatabaseConnectionSettings setting, CredentialsProvider cp) throws Exception {
		String url = setting.getJDBCUrl(); 
		Properties props = new Properties();
		props.setProperty("user", setting.getUserName(cp));
		props.setProperty("password",setting.getPassword(cp));
		Connection conn = DriverManager.getConnection(url, props);
		return conn;
	}

	public static void execQuery(Connection conn, String sql) throws Exception {
		Statement st = conn.createStatement();
		try {
			st.execute(sql);
		} catch (Exception e) {
			st.close();
			throw e;
		}
		
		st.close();
	}
		
	/*
	 * execQueryDataType methods will execute one sql statement, and return a value.
	 * We assume the sql statement is constructed by trusted deepgreen code and it will
	 * indeed return exactly one row, one col of correct data type and the data is not
	 * null.
	 */

	public static boolean execQueryBool(Connection conn, String sql) throws Exception {
		Statement st = conn.createStatement();
		boolean ret = false;
		try {
			ResultSet rs = st.executeQuery(sql); 
			if (rs.next()) {
				ret = rs.getBoolean(1);
			}
		} catch (Exception e) {
			st.close();
			throw e;
		}
		st.close();
		return ret;
	}

	public static int execQueryInt(Connection conn, String sql) throws Exception {
		Statement st = conn.createStatement();
		int ret = 0; 
		try {
			ResultSet rs = st.executeQuery(sql); 
			if (rs.next()) {
				ret = rs.getInt(1); 
			}
		} catch (Exception e) {
			st.close();
			throw e;
		}
		st.close();
		return ret;
	}

	public static long execQueryLong(Connection conn, String sql) throws Exception {
		Statement st = conn.createStatement();
		long ret = 0; 
		try {
			ResultSet rs = st.executeQuery(sql); 
			if (rs.next()) {
				ret = rs.getLong(1); 
			}
		} catch (Exception e) {
			st.close();
			throw e;
		}
		st.close();
		return ret;
	}

	public static double execQueryDouble(Connection conn, String sql) throws Exception {
		Statement st = conn.createStatement();
		double ret = 0.0; 
		try {
			ResultSet rs = st.executeQuery(sql); 
			if (rs.next()) {
				ret = rs.getDouble(1); 
			}
		} catch (Exception e) {
			st.close();
			throw e;
		}
		st.close();
		return ret;
	}

	public static String execQueryString(Connection conn, String sql) throws Exception {
		Statement st = conn.createStatement();
		String ret = ""; 
		try {
			ResultSet rs = st.executeQuery(sql); 
			if (rs.next()) {
				ret = rs.getString(1); 
			}
		} catch (Exception e) {
			st.close();
			throw e;
		}
		st.close();
		return ret;
	}
}
