package com.lifeix.db;

import java.sql.SQLException;



/**
 * 写连接
 * @author neoyin
 *
 */
public class DBWriteConnection extends DBConnection {

	public DBWriteConnection(String dbname) {
		super(dbname);
		try {
			conn.setReadOnly(false);
		} catch (SQLException e) {
			logger.error(e.getMessage(),e);
		}
	}
}
