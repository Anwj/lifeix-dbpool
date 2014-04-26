package com.lifeix.db;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBConfigUtils {
	
	private static Map<String, Properties> dbConfig = new HashMap<String, Properties>(); 
	/**
	 * debug prefix
	 */
	protected static final String DB_LOG_PREFIX ="lifeix-dbpool.log  =====> ";
	/**
	 * 配置文件名
	 */
	private final static String FILE_NAME ="dbpool_config.properties";
	private final static Logger logger = LoggerFactory.getLogger(DBConfigUtils.class);
	
	
	protected static Map<String, Properties> getConfigMap(){
		return dbConfig;
	}
	
	/**
	 * 通过文件名得到相关配置
	 * @param configName
	 * @return
	 */
	protected static Properties getConfigProperties(String configName){
		return dbConfig.get(configName);
	}

	/**
	 * 初始化配置文件 如果没有刚读取default文件
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public static void initDBConfig(String filename) throws FileNotFoundException, IOException{
		String path ="";
		if (filename==null) {
			try {
				path = DBConfigUtils.class.getResource("/").getPath().replaceAll("%20", " ");
			} catch (Exception e) {
				path =System.getProperty("user.dir")+"/";
			}
			filename = path+FILE_NAME;
		}
		logger.info(DB_LOG_PREFIX+"dbconfig file:"+filename);
		Properties properties = new Properties();
		properties.load(new FileInputStream(filename));
		Enumeration enu = properties.propertyNames();
	    while (enu.hasMoreElements()) {
			String key = (String) enu.nextElement();
			String value = properties.getProperty(key);
			if (value.endsWith(".properties")) {
				if (value.indexOf("/")<0) {
					value =  path+value;
				}
				logger.info(DB_LOG_PREFIX+"dbconfig "+key+":"+value);
				
				Properties temp = new Properties();
				temp.load(new FileInputStream(value));
				dbConfig.put(key, temp);
			}
			
	    }
	}
	
	
	/**
	 * 刷新数据库链接配置
	 * @return
	 */
	public static boolean refreshConfig(){
		
		if(dbConfig!=null){
			dbConfig.clear();
			logger.debug(DB_LOG_PREFIX+" DBConfigUtils refreshConfig is ok.");
			return true;
		}
		return false;
	}

}
