package com.lifeix.db;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;


/**
*  连接池管理
*/

public class DBConnectionManager {
	
	private static final Logger logger = LoggerFactory.getLogger(DBConnectionManager.class);
	
	private DBConnectionManager(){
		
	}
	/**
	 * 根据不同项目的放入source
	 */
	private static Map<String,BoneCPDataSource>  sourceMap = new HashMap<String, BoneCPDataSource>();
	
	public static DBConnectionManager getInstance(){
		return SingletonHolder.instance;
	}	
	/**
	 * 单例模式
	 * @author neoyin
	 *
	 */
	private static class SingletonHolder{
		private static DBConnectionManager instance = new DBConnectionManager(); 
	}
	
	/**
	 * 获取读数据库连接
	 * @return
	 * @throws SQLException 
	 */
	protected Connection getConnection(String dbname) throws SQLException{
		BoneCPDataSource dataSource = sourceMap.get(dbname);
		if (dataSource==null) {
			dataSource = getDataSource(dbname);
		}			
		return dataSource.getConnection();
	}
	
	/**
	 * 根据host 初始化BoneCPDataSource
	 * @param host
	 * @return
	 */
	private BoneCPDataSource getDataSource(String dbname){
		try {
			Properties properties = DBConfigUtils.getConfigProperties(dbname);
			if (properties.get("driverClass")!=null) {
				Class.forName(properties.get("driverClass").toString());
			}
			BoneCPConfig config = new BoneCPConfig(properties);
	        BoneCPDataSource dbSource = new BoneCPDataSource(config);
	        sourceMap.put(dbname, dbSource);
			return dbSource;
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(DBConfigUtils.DB_LOG_PREFIX+"ConnectionManager.getDataSource error "+dbname+":"+e.getMessage(),e);
			return null;
		}
	}

	/**
	 * 初始化数据库链接池
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 * @throws SQLException 
	 */
	public void initDBpool(String name) throws FileNotFoundException, IOException, SQLException{
		DBConfigUtils.initDBConfig(name);
		Map<String,Properties> configMap = DBConfigUtils.getConfigMap();
		if (configMap.size()<1) {
			throw new FileNotFoundException("No file about dbpool config ...");
		}
		for (String temp : configMap.keySet()) {
			Connection conn = getConnection(temp);
			conn.createStatement().execute("SELECT 1");
			
			conn.close();
		}
	}
	
	/**
	 * releaseDBSource
	 * @return
	 */
	public boolean releaseDBSource(){
		//刷新配置
		if (sourceMap.size()>0) {
			for (BoneCPDataSource source : sourceMap.values()) {
				if (source!=null) {
					source.close();
				}
			}
			sourceMap.clear();
			logger.info(" relase db source ");
		}
		return true;
	}
	
	
}