package com.lifeix.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class DaoImplBase<T>{
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DaoImplBase.class);
	
	/**
	 * release数据库链接1
	 * @param rs
	 * @param conn
	 */
	protected void closeDBConnection(ResultSet rs , IntfDBConnection conn) {
		try {
			if (rs != null) {
				rs.close();
				rs = null;
			}
			if (conn != null) {
				conn.release();
				conn = null;
			}
		} catch (SQLException e) {
			LOGGER.error(e.getMessage(),e);
		}
	}

	public T findById(String sql,long id) {
		IntfDBConnection conn = getConnection();
		ResultSet rs = null;
		T entity = null;
		try {
			rs = conn.queryByPreparedStatement(sql, id);
			if (rs.next()) {
				entity = getTableDataEntityByRS(rs);
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		} finally {
			closeDBConnection(rs, conn);
		}
		return entity;
	}
	
	/**
	 * 执行查询count的语句<br>
	 * 传入的sql必须是 select count(1) as count from ... 形式
	 * 
	 * @param sql
	 * @param args
	 * @return
	 */
	protected long queryCount(String sql, Object... args) {
		long count = 0;
		ResultSet rs = null;
		IntfDBConnection conn = getConnection();
		try {
			rs = conn.queryByPreparedStatement(sql, args);
			if(rs.next()) {
				count = rs.getInt(1);
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		} finally {
			closeDBConnection(rs, conn);
		}
		return count;
	}
	
	/**
	 * 插入一条记录，并返回ID
	 * @param entity
	 * @return
	 */
	protected long insertAndGetPrimarykeyByPreparedStatement(String sql, Object... args) {
		IntfDBConnection conn = getConnection();
		long id = Long.valueOf(-1);
		try {
			id = conn.insertAndGetPrimarykeyByPreparedStatement(sql, args);
			return id;
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			return id;
		} finally {
			closeDBConnection(null, conn);
		}
	}
	/**
	 * 根据ID删除一条记录
	 * 
	 * @param id
	 * @param updateCache
	 *            是否更新缓存
	 * @return
	 */
	public boolean deleteById(String sql) {
		
		IntfDBConnection conn = getConnection();
		boolean flag = false;
		try {
			flag = conn.deleteByPreparedStatement(sql);
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		} finally {
			closeDBConnection(null, conn);
		}
		return flag;
	}
	
	/**
	 * update 
	 * @param sql
	 * @param args
	 * @return
	 */
	protected boolean updateByPreparedStatement(String sql, Object... args) {
		IntfDBConnection conn = getConnection();
		boolean flag = false;
		try {
			flag = conn.updateByPreparedStatement(sql, args);

		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		} finally {
			closeDBConnection(null, conn);
		}
		return flag;
	}
	/**
	 * 执行查询语句
	 * 
	 * @param sql
	 * @param args
	 * @return
	 */
	protected List<T> queryByPreparedStatement(String sql, Object... args) {

		ResultSet rs = null;
		IntfDBConnection conn = getConnection();
		List<T> list = new ArrayList<T>();
		try {
			rs = conn.queryByPreparedStatement(sql, args);
			while (rs.next()) {
				T entity = getTableDataEntityByRS(rs);
				list.add(entity);
			}
			return list;
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			return list;
		} finally {
			closeDBConnection(rs, conn);
		}
	}
	
	public T getTableDataEntityByRS(ResultSet rs) throws SQLException{
		return null;
	}
	
	public abstract IntfDBConnection getConnection();
	
}
