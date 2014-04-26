package com.lifeix.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import javax.sql.rowset.CachedRowSet;

public interface IntfDBConnection {
	
	/**
	 * 插入并获取主键
	 * @param sql
	 * @return {@link Long} 插入成功后返回主键id
	 * 
	 */
	Long insertAndGetPrimarykeyByStatement(String sql)throws SQLException;
	
	/**
	 * 插入数据
	 * @param sql
	 * @return {@link Boolean} 成功返回true,失败返回false
	 */
	boolean insertByStatement(String sql)throws SQLException;
	
	/**
	 * 更新
	 * @param sql
	 * @return {@link Boolean} 成功返回true,失败返回false
	 */
	boolean updateByStatement(String sql)throws SQLException;
	
	/**
	 * 删除
	 * @param sql
	 * @return {@link Boolean} 成功返回true,失败返回false
	 */
	boolean deleteByStatement(String sql)throws SQLException;
	
	/**
	 * 批量执行sql
	 * @param sqlList
	 * @return {@link Boolean} 成功返回true,失败返回false
	 */
	boolean executeBatchByStatement(List<String> sqlList)throws SQLException;
	
	/**
	 * 查询
	 * @param sql
	 * @return {@link CachedRowSet} 
	 */
	CachedRowSet queryByStatement(String sql)throws SQLException;
	
	/**
	 * 插入数据
	 * @param sql
	 * @param args
	 * @return {@link Long}
	 */
	Long insertAndGetPrimarykeyByPreparedStatement(String sql, Object ... args)throws SQLException;
	
	/**
	 * 插入
	 * @param sql
	 * @param args
	 * @return {@link Boolean}
	 */
	boolean insertByPreparedStatement(String sql, Object ... args)throws SQLException;
	
	/**
	 * 更新
	 * @param sql
	 * @param args
	 * @return {@link Boolean} 
	 */
	boolean updateByPreparedStatement(String sql, Object ... args)throws SQLException;
	
	/**
	 * 删除
	 * @param sql
	 * @param args
	 * @return {@link Boolean}
	 */
	boolean deleteByPreparedStatement(String sql, Object ... args)throws SQLException;
	
	/**
	 * 批量执行sql
	 * @param sql
	 * @param times
	 * @param args
	 * @return {@link Boolean}
	 */
	boolean executeBatchByPreparedStatement(String sql,int times, Object ... args )throws SQLException;
	
	/**
	 * 查询
	 * @param sql
	 * @param args
	 * @return {@link CachedRowSet}
	 */
	CachedRowSet queryByPreparedStatement(String sql, Object ... args)throws SQLException;
	
	/**
	 * @param sql
	 * @return {@link PreparedStatement}
	 */
	PreparedStatement createPreparedStatement(String sql)throws SQLException;
	
	/**
	 * 
	 * @return {@link Statement}
	 */
	Statement createStatement()throws SQLException;
	
	/**
	 * 
	 * @param sql
	 * @return {@link PreparedStatement}
	 */
	PreparedStatement preparedStatement(String sql)throws SQLException;
	
	/**
	 * 执行函数或sp
	 * @param sql
	 * @return {@link Boolean}
	 */
	boolean callByStatement(String sql)throws SQLException;
	
	/**
	 *  执行函数或sp（更新操作）
	 * @param sql
	 * @return {@link Boolean}
	 */
	boolean callUpdateStatement(String sql)throws SQLException;
	
	/**
	 * 执行函数或sp（更新操作）
	 * @param sql
	 * @param args
	 * @return
	 */
	boolean callUpdateByPreparedStatement(String sql, Object ...args)throws SQLException;
	
	/**
	 * 执行函数或sp
	 * @param sql
	 * @param args
	 * @return {@link Boolean}
	 */
	boolean callByPreparedStatement(String sql , Object ... args)throws SQLException;
	
	/**
	 * 执行函数或sp(带result返回)
	 * @param sql
	 * @return {@link CachedRowSet}
	 */
	CachedRowSet callByStatementReturnResult(String sql)throws SQLException;
	
	/**
	 * 执行函数或sp(带result返回)
	 * @param sql
	 * @param args
	 * @return {@link CachedRowSet}
	 */
	CachedRowSet callByPreparedStatementReturnResult(String sql, Object ... args)throws SQLException;
	
	/**
	 * 释放链接
	 * @return
	 */
	void release();
	
	/**
	 * 开始事务
	 */
	void beginTransaction();
	
	/**
	 * 结束事务
	 */
	void endTransaction();
    
	/**
	 * 回滚事务
	 */
	void rollback();

	/**
	 * 取得事务级别
	 * @return
	 */
	int getTransactionIsolation();
	
	/**
	 * 设置事务级别
	 * @param level
	 */
	void setTransactionIsolation(int level);
}
