package com.lifeix.db;



import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.jdbc.Driver;
import com.mysql.jdbc.PingTarget;

/**
 * @author ahuaxuan(aaron zhang)
 * @since 2008-6-18
 * @version $Id$
 */
public class EasyReplicationConnection implements Connection, PingTarget {

	private static transient Logger logger = LoggerFactory.getLogger(EasyReplicationConnection.class);
	
	protected com.mysql.jdbc.Connection currentConnection;
	protected com.mysql.jdbc.Connection masterConnection;
	protected com.mysql.jdbc.Connection slavesConnection;

	public EasyReplicationConnection(Properties masterProperties,
			Properties slaveProperties) throws SQLException {
		Driver driver = new Driver();
		
		StringBuffer masterUrl = new StringBuffer("jdbc:mysql://");
		StringBuffer slaveUrl = new StringBuffer("jdbc:mysql://");
		String masterHost = masterProperties.getProperty("HOST");
		if (masterHost != null) {
			masterUrl.append(masterHost);
		}
		String slaveHost = slaveProperties.getProperty("HOST");
		if (slaveHost != null) {
			slaveUrl.append(slaveHost);
		}
		String masterDb = masterProperties.getProperty("DBNAME");
		masterUrl.append("/");
		if (masterDb != null) {
			masterUrl.append(masterDb);
		}
		String slaveDb = slaveProperties.getProperty("DBNAME");
		slaveUrl.append("/");
		if (slaveDb != null) {
			slaveUrl.append(slaveDb);
		}
		
		//从这里可以看出，笔者前文提出的猜想是正确的，每一个ReplicationDriver其实是两个Connection的代理，这两个
		//Connection才是真正访问DB的connection。
		masterConnection = (com.mysql.jdbc.Connection) driver.connect(masterUrl
				.toString(), masterProperties);
		
		if (slaveUrl.toString().contains("///")) {
			if (logger.isDebugEnabled()) {
				logger.debug(" ----- the salveUrl contains the '///', " +
						"that means there is no slaver, make slavesConnection = masterConnection --");
			}
			slavesConnection = masterConnection;
		} else {
			slavesConnection = (com.mysql.jdbc.Connection) driver.connect(slaveUrl
					.toString(), slaveProperties);
		}
		currentConnection = masterConnection;
	}

	public synchronized void clearWarnings() throws SQLException {
		currentConnection.clearWarnings();
	}

	public synchronized void close() throws SQLException {
		masterConnection.close();
		slavesConnection.close();
	}

	public synchronized void commit() throws SQLException {
		currentConnection.commit();
	}

	public Statement createStatement() throws SQLException {
		Statement stmt = currentConnection.createStatement();
		((com.mysql.jdbc.Statement) stmt).setPingTarget(this);
		return stmt;
	}

	public synchronized Statement createStatement(int resultSetType,
			int resultSetConcurrency) throws SQLException {
		Statement stmt = currentConnection.createStatement(resultSetType,
				resultSetConcurrency);
		((com.mysql.jdbc.Statement) stmt).setPingTarget(this);
		return stmt;
	}

	public synchronized Statement createStatement(int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		Statement stmt = currentConnection.createStatement(resultSetType,
				resultSetConcurrency, resultSetHoldability);
		((com.mysql.jdbc.Statement) stmt).setPingTarget(this);
		return stmt;
	}

	public synchronized boolean getAutoCommit() throws SQLException {
		return currentConnection.getAutoCommit();
	}

	public synchronized String getCatalog() throws SQLException {
		return currentConnection.getCatalog();
	}

	public synchronized com.mysql.jdbc.Connection getCurrentConnection() {
		return currentConnection;
	}

	public synchronized int getHoldability() throws SQLException {
		return currentConnection.getHoldability();
	}

	public synchronized com.mysql.jdbc.Connection getMasterConnection() {
		return masterConnection;
	}

	public synchronized DatabaseMetaData getMetaData() throws SQLException {
		return currentConnection.getMetaData();
	}

	public synchronized com.mysql.jdbc.Connection getSlavesConnection() {
		return slavesConnection;
	}

	public synchronized int getTransactionIsolation() throws SQLException {
		return currentConnection.getTransactionIsolation();
	}

	public synchronized Map getTypeMap() throws SQLException {
		return currentConnection.getTypeMap();
	}

	public synchronized SQLWarning getWarnings() throws SQLException {
		return currentConnection.getWarnings();
	}

	public synchronized boolean isClosed() throws SQLException {
		return currentConnection.isClosed();
	}

	public synchronized boolean isReadOnly() throws SQLException {
		return currentConnection == slavesConnection;
	}

	public synchronized String nativeSQL(String sql) throws SQLException {
		return currentConnection.nativeSQL(sql);
	}

	public CallableStatement prepareCall(String sql) throws SQLException {
		return currentConnection.prepareCall(sql);
	}

	public synchronized CallableStatement prepareCall(String sql,
			int resultSetType, int resultSetConcurrency) throws SQLException {
		return currentConnection.prepareCall(sql, resultSetType,
				resultSetConcurrency);
	}

	public synchronized CallableStatement prepareCall(String sql,
			int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		return currentConnection.prepareCall(sql, resultSetType,
				resultSetConcurrency, resultSetHoldability);
	}

	public PreparedStatement prepareStatement(String sql) throws SQLException {
		PreparedStatement pstmt = currentConnection.prepareStatement(sql);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		return pstmt;
	}

	public synchronized PreparedStatement prepareStatement(String sql,
			int autoGeneratedKeys) throws SQLException {
		PreparedStatement pstmt = currentConnection.prepareStatement(sql,
				autoGeneratedKeys);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		return pstmt;
	}

	public synchronized PreparedStatement prepareStatement(String sql,
			int resultSetType, int resultSetConcurrency) throws SQLException {
		PreparedStatement pstmt = currentConnection.prepareStatement(sql,
				resultSetType, resultSetConcurrency);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		return pstmt;
	}

	public synchronized PreparedStatement prepareStatement(String sql,
			int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		PreparedStatement pstmt = currentConnection.prepareStatement(sql,
				resultSetType, resultSetConcurrency, resultSetHoldability);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		return pstmt;
	}

	public synchronized PreparedStatement prepareStatement(String sql,
			int columnIndexes[]) throws SQLException {
		PreparedStatement pstmt = currentConnection.prepareStatement(sql,
				columnIndexes);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		return pstmt;
	}

	public synchronized PreparedStatement prepareStatement(String sql,
			String columnNames[]) throws SQLException {
		PreparedStatement pstmt = currentConnection.prepareStatement(sql,
				columnNames);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		return pstmt;
	}

	public synchronized void releaseSavepoint(Savepoint savepoint)
			throws SQLException {
		currentConnection.releaseSavepoint(savepoint);
	}

	public synchronized void rollback() throws SQLException {
		currentConnection.rollback();
	}

	public synchronized void rollback(Savepoint savepoint) throws SQLException {
		currentConnection.rollback(savepoint);
	}

	public synchronized void setAutoCommit(boolean autoCommit)
			throws SQLException {
		currentConnection.setAutoCommit(autoCommit);
	}

	public synchronized void setCatalog(String catalog) throws SQLException {
		currentConnection.setCatalog(catalog);
	}

	public synchronized void setHoldability(int holdability)
			throws SQLException {
		currentConnection.setHoldability(holdability);
	}

	public synchronized void setReadOnly(boolean readOnly) throws SQLException {
		if (readOnly) {
			if (currentConnection != slavesConnection) {
				switchToSlavesConnection();
			}
		} else if (currentConnection != masterConnection) {
			switchToMasterConnection();
		}
	}

	public synchronized Savepoint setSavepoint() throws SQLException {
		return currentConnection.setSavepoint();
	}

	public synchronized Savepoint setSavepoint(String name) throws SQLException {
		return currentConnection.setSavepoint(name);
	}

	public synchronized void setTransactionIsolation(int level)
			throws SQLException {
		currentConnection.setTransactionIsolation(level);
	}

	public synchronized void setTypeMap(Map arg0) throws SQLException {
		currentConnection.setTypeMap(arg0);
	}

	private synchronized void switchToMasterConnection() throws SQLException {
		swapConnections(masterConnection, slavesConnection);
	}

	private synchronized void switchToSlavesConnection() throws SQLException {
		swapConnections(slavesConnection, masterConnection);
	}

	private synchronized void swapConnections(
			com.mysql.jdbc.Connection switchToConnection,
			com.mysql.jdbc.Connection switchFromConnection) throws SQLException {
		String switchFromCatalog = switchFromConnection.getCatalog();
		String switchToCatalog = switchToConnection.getCatalog();
		if (switchToCatalog != null
				&& !switchToCatalog.equals(switchFromCatalog)) {
			switchToConnection.setCatalog(switchFromCatalog);
		} else if (switchFromCatalog != null) {
			switchToConnection.setCatalog(switchFromCatalog);
		}
		boolean switchToAutoCommit = switchToConnection.getAutoCommit();
		boolean switchFromConnectionAutoCommit = switchFromConnection
				.getAutoCommit();
		if (switchFromConnectionAutoCommit != switchToAutoCommit) {
			switchToConnection.setAutoCommit(switchFromConnectionAutoCommit);
		}
		int switchToIsolation = switchToConnection.getTransactionIsolation();
		int switchFromIsolation = switchFromConnection
				.getTransactionIsolation();
		if (switchFromIsolation != switchToIsolation) {
			switchToConnection.setTransactionIsolation(switchFromIsolation);
		}
		currentConnection = switchToConnection;
	}

	public synchronized void doPing() throws SQLException {
		if (masterConnection != null) {
			masterConnection.ping();
		}
		if (slavesConnection != null) {
			slavesConnection.ping();
		}
	}

	public Array createArrayOf(String typeName, Object[] elements)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Blob createBlob() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Clob createClob() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public NClob createNClob() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public SQLXML createSQLXML() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Struct createStruct(String typeName, Object[] attributes)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Properties getClientInfo() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public String getClientInfo(String name) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean isValid(int timeout) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public void setClientInfo(Properties properties)
			throws SQLClientInfoException {
		// TODO Auto-generated method stub

	}

	public void setClientInfo(String name, String value)
			throws SQLClientInfoException {
		// TODO Auto-generated method stub

	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
}
