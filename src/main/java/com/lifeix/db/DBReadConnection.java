package com.lifeix.db;


/**
 * 读连接
 * @author neoyin
 *
 */
public class DBReadConnection extends DBConnection{

	public DBReadConnection(String dbname) {
		super(dbname);
		try {
			conn.setReadOnly(true);
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
		
	}

}
