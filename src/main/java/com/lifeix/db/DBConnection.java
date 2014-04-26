package com.lifeix.db;

import java.sql.BatchUpdateException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.sql.rowset.CachedRowSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.rowset.CachedRowSetImpl;

public abstract class DBConnection implements IntfDBConnection {
	protected static final Logger logger = LoggerFactory.getLogger(DBConnection.class);

	protected Connection conn = null;
	
	/**
	 * 写操作
	 */
	public DBConnection(String dbname) {
		try {
			conn = DBConnectionManager.getInstance().getConnection(dbname);
		} catch (Exception e) {
			logger.debug(e.getMessage(),e);
		}
	}
	
	public Connection getConnection(){
		return conn;
	}
	

	public void release() {
		if (conn != null) {
			try {
				conn.close();
				conn = null;
			} catch (SQLException e) {
				logger.error(e.getMessage(), e);
			}
		}
	}

	/**
	 * @param sql
	 * @return {@link PreparedStatement}
	 */
	public PreparedStatement createPreparedStatement(String sql) {
		if (conn != null) {
			try {
				return conn.prepareStatement(sql);
			} catch (SQLException e) {
				logger.error(e.getMessage(), e);
			}
		}
		return null;
	}
	
	public PreparedStatement preparedStatement(String sql) {
		if (conn != null) {
			try {
				return conn.prepareStatement(sql,PreparedStatement.RETURN_GENERATED_KEYS);
			} catch (SQLException e) {
				logger.error(e.getMessage(), e);
			}
		}
		return null;
	}
	
	public Statement createStatement() {
		if (conn != null) {
			try {
				return conn.createStatement();
			} catch (SQLException e) {
				logger.error(e.getMessage(), e);
			}
		}
		return null;
	}
	
	/**
	 * @throws SQLException 
	 * 
	 */
	public boolean deleteByPreparedStatement(String sql, Object... args) throws SQLException {
		boolean ret = false;
		if (conn != null) {
			PreparedStatement ps = null;
			try {
				// 如果是直读 ---直接return;
				if(conn.isReadOnly()){
					throw new SQLException("this is slave conn  ,you can't execution deleteByPreparedStatement operation ");
				}
				
				ps = conn.prepareStatement(sql);
				int sz = args.length;
				for (int i = 0; i < sz; i++) {
					ps.setObject(i + 1, args[i]);
				}
				int rt = ps.executeUpdate();
				if (rt > 0)
					ret = true;
			} finally {
				closePs(ps);
			}
		}
		return ret;
	}

	/**
	 * 删除操作
	 * @param sql
	 * @return {@link Boolean} 成功返回true,失败返回false
	 * @throws SQLException 
	 */
	public boolean deleteByStatement(String sql) throws SQLException {
		boolean ret = false;
		if (conn != null) {
			Statement stmt = null;
			try {
				// 如果是直读 ---直接return;
				if(conn.isReadOnly()){
					throw new SQLException("this is slave conn  ,you can't execution deleteByStatement operation ");
				}
				
				stmt = conn.createStatement();
				int rt = stmt.executeUpdate(sql);
				if (rt > 0)
					ret = true;
			} finally {
				closeStmt(stmt);
			}
		}
		return ret;
	}

	/**
	 * 批量执行sql
	 * @param sql
	 * @param times
	 * @param args
	 * @return {@link Boolean}
	 */
	public boolean executeBatchByPreparedStatement(String sql, int times,Object... args) {
		boolean ret = false;
		if (conn != null) {
			PreparedStatement ps = null;
			try {
				// 如果是直读 ---直接return;
				if(conn.isReadOnly()){
					throw new SQLException("this is slave conn  ,you can't execution executeBatchByPreparedStatement operation ");
				}
				
				conn.setAutoCommit(false);
				ps = conn.prepareStatement(sql);
				int sz = args.length / times;
				for (int i = 0; i < times; i++) {
					for (int j = 0; j < sz; j++) {
						ps.setObject(j + 1, args[sz * i + j]);
					}
					ps.addBatch();
				}
				ps.executeBatch();
				conn.commit();
				ret = true;
			} catch (BatchUpdateException e) {
				try {
					conn.rollback();
				} catch (SQLException e1) {
					logger.error(e1.getMessage(), e1);
				}
			} catch (SQLException e) {
				logger.error(e.getMessage(), e);
			} finally {
				try {
					conn.setAutoCommit(true);
					if (ps != null) {
						ps.close();
						ps = null;
					}
				} catch (SQLException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
		return ret;
	}

	/**
	 * 批量执行sql
	 * @param sqlList
	 * @return {@link Boolean} 成功返回true,失败返回false
	 * @throws SQLException 
	 */
	public boolean executeBatchByStatement(List<String> sqlList) throws SQLException {
		boolean ret = false;
		if (conn != null) {
			Statement stmt = null;
			try {
				// 如果是直读 ---直接return;
				if(conn.isReadOnly()){
					throw new SQLException("this is slave conn  ,you can't execution executeBatchByStatement operation ");
				}
				conn.setAutoCommit(false);
				stmt = conn.createStatement();
				for (String s : sqlList) {
					stmt.addBatch(s);
				}
				stmt.executeBatch();
				conn.commit();
				ret = true;
			} catch (Exception e) {
				logger.error(e.getMessage(),e);
				conn.rollback();
			} finally {
				try {
					conn.setAutoCommit(true);
				} catch (SQLException e) {
					logger.error(e.getMessage(), e);
				}
				closeStmt(stmt);
			}
		}
		return ret;
	}

	/**
	 * 插入数据
	 * @param sql
	 * @param args
	 * @return {@link Long}
	 * @throws SQLException 
	 */
	public Long insertAndGetPrimarykeyByPreparedStatement(String sql,Object... args) throws SQLException {
		Long ret = Long.valueOf(-1);
		if (conn != null) {
			PreparedStatement ps = null;
			try {
				// 如果是直读 ---直接return;
				if(conn.isReadOnly()){
					throw new SQLException("this is slave conn  ,you can't execution insertAndGetPrimarykeyByPreparedStatement operation ");
				}
				
				ps = conn.prepareStatement(sql,PreparedStatement.RETURN_GENERATED_KEYS);
				int sz = args.length;
				for (int i = 0; i < sz; i++) {
					ps.setObject(i + 1, args[i]);
				}
				ps.execute();
				ResultSet rs = ps.getGeneratedKeys();
				if (rs.next())
					ret = rs.getLong(1);
				
			} finally {
				closePs(ps);
			}
		}
		return ret;
	}

	/**
	 * 插入并获取主键
	 * @throws SQLException 
	 */
	public Long insertAndGetPrimarykeyByStatement(String sql) throws SQLException {
		Long ret = Long.valueOf(-1);
		if (conn != null) {
			Statement stmt = null;
			try {
				// 如果是直读 ---直接return;
				if(conn.isReadOnly()){
					throw new SQLException("this is slave conn  ,you can't execution insertAndGetPrimarykeyByStatement operation ");
				}
				stmt = conn.createStatement();
				stmt.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
				ResultSet rs = stmt.getGeneratedKeys();
				if (rs.next())
					ret = rs.getLong(1);
				rs.close();
				rs = null;
			} finally {
				try {
					if (stmt != null) {
						stmt.close();
						stmt = null;
					}
				} catch (SQLException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
		return ret;
	}

	/**
	 * 插入
	 * @param sql
	 * @param args
	 * @return {@link Boolean}
	 * @throws SQLException 
	 */
	public boolean insertByPreparedStatement(String sql, Object... args) throws SQLException {
		boolean ret = false;
		if (conn != null) {
			PreparedStatement ps = null;
			try {
				// 如果是直读 ---直接return;
				if(conn.isReadOnly()){
					throw new SQLException("this is slave conn  ,you can't execution insertByPreparedStatement operation ");
				}
				
				ps = conn.prepareStatement(sql);
				int sz = args.length;
				for (int i = 0; i < sz; i++) {
					ps.setObject(i + 1, args[i]);
				}
				int rt = ps.executeUpdate();
				if (rt > 0)
					ret = true;
			} finally {
				closePs(ps);
			}
		}
		return ret;
	}

	/**
	 * 插入数据
	 * @param sql
	 * @return {@link Boolean} 成功返回true,失败返回false
	 * @throws SQLException 
	 */
	public boolean insertByStatement(String sql) throws SQLException {
		boolean ret = false;
		if (conn != null) {
			Statement stmt = null;
			try {
				// 如果是直读 ---直接return;
				if(conn.isReadOnly()){
					throw new SQLException("this is slave conn  ,you can't execution insertByStatement operation ");
				}
				stmt = conn.createStatement();
				int rt = stmt.executeUpdate(sql);
				if (rt > 0)
					ret = true;
			} finally {
				closeStmt(stmt);
			}
		}
		return ret;
	}

	/**
	 * 查询
	 * @param sql
	 * @param args
	 * @return {@link CachedRowSet}
	 * @throws SQLException 
	 */
	public CachedRowSet queryByPreparedStatement(String sql, Object... args) throws SQLException {
		CachedRowSet ret = null;
		if (conn != null) {
			PreparedStatement ps = null;
			try {
				ps = conn.prepareStatement(sql);
				int sz = args.length;
				for (int i = 0; i < sz; i++) {
					ps.setObject(i + 1, args[i]);
				}
				ResultSet rs = ps.executeQuery();
				ret = new CachedRowSetImpl();
				ret.populate(rs);
				rs.close();
				rs = null;				
			} finally {
				closePs(ps);
			}
		}
		return ret;
	}
	

	/**
	 * 查询
	 * @param sql
	 * @return {@link CachedRowSet} 
	 * @throws SQLException 
	 */
	public CachedRowSet queryByStatement(String sql) throws SQLException {
		CachedRowSet ret = null;
		if (conn != null) {
			Statement stmt = null;
			try {
				stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery(sql);
				ret = new CachedRowSetImpl();
				ret.populate(rs);
				rs.close();
				rs = null;
			} finally {
				try {
					if (stmt != null) {
						stmt.close();
						stmt = null;
					}
				} catch (SQLException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
		return ret;
	}

	/**
	 * 更新
	 * @param sql
	 * @param args
	 * @return {@link Boolean} 
	 * @throws SQLException 
	 */
	public boolean updateByPreparedStatement(String sql, Object... args) throws SQLException {
		boolean ret = false;
		if (conn != null) {
			PreparedStatement ps = null;
			try {
				// 如果是直读 ---直接return;
				if(conn.isReadOnly()){
					throw new SQLException("this is slave conn  ,you can't execution updateByPreparedStatement operation ");
				}
				
				ps = conn.prepareStatement(sql);
				int sz = args.length;
				for (int i = 0; i < sz; i++) {
					ps.setObject(i + 1, args[i]);
				}
				int rt = ps.executeUpdate();
				if (rt > 0)
					ret = true;

			} finally {
				closePs(ps);
			}
		}
		return ret;
	}

	/**
	 * 更新操作
	 * @param sql
	 * @return 成功返回true,失败返回false
	 * @throws SQLException 
	 */
	public boolean updateByStatement(String sql) throws SQLException {
		boolean ret = false;
		if (conn != null) {
			Statement stmt = null;
			try {
				// 如果是直读 ---直接return;
				if(conn.isReadOnly()){
					throw new SQLException("this is slave conn  ,you can't execution updateByStatement operation ");
				}
				stmt = conn.createStatement();
				int rt = stmt.executeUpdate(sql);
				if (rt > 0)
					ret = true;
			} finally {
				closeStmt(stmt);
			}
		}
		return ret;
	}
	
	/**
	 * 执行函数或sp
	 * @param sql
	 * @param args
	 * @return {@link Boolean}
	 * @throws SQLException 
	 */
	public boolean callByPreparedStatement(String sql, Object... args) throws SQLException {
		boolean ret = false;
		if (conn != null) {
			CallableStatement cs = null;
			try {
				// 如果是直读 ---直接return;
				if(conn.isReadOnly()){
					throw new SQLException("this is slave conn  ,you can't execution callByPreparedStatement operation ");
				}
				cs = conn.prepareCall(sql);
				for (int i = 0; i < args.length; i++) {
					cs.setObject(i + 1, args[i]);
				}
				ret = cs.execute();
			} finally {
				closeCs(cs);
			}
		}
		return ret;
	}

	/**
	 * 执行函数或sp(带result返回)
	 * @param sql
	 * @param args
	 * @return {@link CachedRowSet}
	 * @throws SQLException 
	 */
	public CachedRowSet callByPreparedStatementReturnResult(String sql, Object... args) throws SQLException {
		CachedRowSet ret = null;
		if (conn != null) {
			CallableStatement cs = null;
			try {
				// 如果是直读 ---直接return;
				if(conn.isReadOnly()){
					throw new SQLException("this is slave conn  ,you can't execution callByPreparedStatementReturnResult operation ");
				}
				cs = conn.prepareCall(sql);
				for (int i = 0; i < args.length; i++) {
					cs.setObject(i + 1, args[i]);
				}
				ResultSet rs = cs.executeQuery();
				ret = new CachedRowSetImpl();
				ret.populate(rs);
				rs.close();
				rs = null;
			} finally {
				closeCs(cs);
			}
			
		}
		return ret;
	}


	/**
	 * 执行函数或sp
	 * @param sql
	 * @return {@link Boolean}
	 * @throws SQLException 
	 */
	public boolean callByStatement(String sql) throws SQLException {
		boolean ret = false;
		if (conn != null) {
			CallableStatement cs = null;
			try {
				// 如果是直读 ---直接return;
				if(conn.isReadOnly()){
					throw new SQLException("this is slave conn  ,you can't execution callByStatement operation ");
				}
				cs = conn.prepareCall(sql);
				ret = cs.execute();
			} finally {
				closeCs(cs);
			}
		}
		return ret;
	}

	/**
	 * 执行函数或sp(带result返回)
	 * @param sql
	 * @return {@link CachedRowSet}
	 * @throws SQLException 
	 */
	public CachedRowSet callByStatementReturnResult(String sql) throws SQLException {
		CachedRowSet ret = null;
		if (conn != null) {
			CallableStatement cs = null;
			try {
				// 如果是直读 ---直接return;
				if(conn.isReadOnly()){
					throw new SQLException("this is slave conn  ,you can't execution callByStatementReturnResult operation ");
				}
				cs = conn.prepareCall(sql);
				ResultSet rs = cs.executeQuery();
				ret = new CachedRowSetImpl();
				ret.populate(rs);
				rs.close();
				rs = null;
			} finally {
				closeCs(cs);
			}
		}
		return ret;
	}
	

	/**
	 * 执行函数或sp（更新操作）
	 * @param sql
	 * @param args
	 * @return
	 * @Override
	 */
	public boolean callUpdateByPreparedStatement(String sql, Object... args)throws SQLException {
		boolean ret = false;
		if (conn != null) {
			CallableStatement cs = null;
			try {
				// 如果是直读 ---直接return;
				if(conn.isReadOnly()){
					throw new SQLException("this is slave conn  ,you can't execution callUpdateByPreparedStatement operation ");
				}
				cs = conn.prepareCall(sql);
				for (int i = 0; i < args.length; i++) {
					cs.setObject(i + 1, args[i]);
				}
				int rt = cs.executeUpdate();
				if (rt > 0)
					ret = true;
			}finally {
				closeCs(cs);
			}
		}
		return ret;
	}
	/**
	 *  执行函数或sp（更新操作）
	 * @param sql
	 * @return {@link Boolean}
	 * @throws SQLException 
	 * @Override
	 */
	public boolean callUpdateStatement(String sql) throws SQLException {
		boolean ret = false;
		if (conn != null) {
			CallableStatement cs = null;
			try {
				if(conn.isReadOnly()){
					throw new SQLException("this is slave conn  ,you can't execution callUpdateStatement operation ");
				}
				// 如果是直读 ---直接return;
				cs = conn.prepareCall(sql);
				int rt = cs.executeUpdate();
				if (rt > 0)
					ret = true;
			}finally {
				closeCs(cs);
			}
		}
		return ret;
	}

	/**
	 * 开始事务
	 */
	public void beginTransaction() {
		try {
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 结束事务
	 */
	public void endTransaction() {
		try {
			conn.commit();
			conn.setAutoCommit(true);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 回滚
	 */
	public void rollback() {
		try {
			conn.rollback();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	/**
	 * 关闭ps
	 * @param ps
	 */
	private void closePs(PreparedStatement ps){
		try {
			if (ps != null) {
				ps.close();
				ps = null;
			}
		} catch (SQLException e) {
			logger.error(" ps close error "+e.getMessage(), e);
		}
	}
	/**
	 * 关闭cs
	 * @param cs
	 */
	private void closeCs(CallableStatement cs){
		try {
			if (cs != null) {
				cs.close();
				cs = null;
			}
		} catch (SQLException e) {
			logger.error("callableStatement close exception"+e.getMessage(),e);
		}
	}
	
	private void closeStmt(Statement stmt){
		try {
			if (stmt != null) {
				stmt.close();
				stmt = null;
			}
		} catch (SQLException e) {
			logger.error("Statement close Exception"+e.getMessage(), e);
		}
	}
	
	
	
	/**
	 * 打印出语句
	 * @param sql
	 * @param params
	 * @return
	 */
	/*private static String getPreparedSQL(String sql, Object... params) {
		 try {
			 //1 如果没有参数，说明是不是动态SQL语句
	          int paramNum = 0;
	                  if (null != params) paramNum = params.length;
	          if (1 > paramNum) return sql;
	         //2 如果有参数，则是动态SQL语句
	          StringBuffer returnSQL = new StringBuffer();
	          String[] subSQL = sql.split("\\?");
	          for (int i = 0; i < paramNum; i++) {
	              if (params[i] instanceof Date) {
	                  returnSQL.append(subSQL[i]).append(" '").append(params[i].toString()).append("' ");
	             } else {
	                  returnSQL.append(subSQL[i]).append(" '").append(params[i]).append("' ");
	              }
	          }
	          if (subSQL.length > params.length) {
	              returnSQL.append(subSQL[subSQL.length - 1]);
	          }
	         return returnSQL.toString();
		} catch (Exception e) {
			logger.debug(e.getMessage(),e);
		}
		return null;
	 }*/

	public int getTransactionIsolation() {
		if (conn != null) {
			try {
				return conn.getTransactionIsolation();
			} catch (SQLException e) {
				logger.error("getTransactionIsolation Exception | " + e.getMessage(), e);
			}
		}
		return 0;
	}

	public void setTransactionIsolation(int level) {
		if (conn != null) {
			try {
				conn.setTransactionIsolation(level);
			} catch (SQLException e) {
				logger.error("setTransactionIsolation Exception | " + e.getMessage(), e);
			}
		}
	}

}
