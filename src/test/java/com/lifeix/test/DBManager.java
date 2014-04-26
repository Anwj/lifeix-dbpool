package com.lifeix.test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;

public class DBManager {

	private static BoneCPDataSource cpools = null;
	
	private DBManager(){
		
	}
	
	public static DBManager getInstance(){
		return InstanceHolder.INSTANCE;
	}
	
	private static class InstanceHolder{
		private static final DBManager INSTANCE = new DBManager();
	}
	

	
	public static void main(String[] args) throws Exception {
		
		//EasyNonRegisteringReplicationDriver driver = new EasyReplicationDriver();

		
		Class.forName("com.mysql.jdbc.ReplicationDriver");
		
		
		Properties props = new Properties();

	    // We want this for failover on the slaves
	    props.put("autoReconnect", "true");

	    // We want to load balance between the slaves
	    props.put("roundRobinLoadBalance", "true");

	    props.put("username", "sneak");
	    props.put("password", "skst");
		
		//Class.forName("com.mysql.jdbc.ReplicationDriver");
		BoneCPConfig config = new BoneCPConfig(props);
		//config.setDriverProperties(driverProperties);
		
		config.setJdbcUrl("jdbc:mysql:replication://192.168.1.245,192.168.1.15/lifeix_sneak?useUnicode=true&amp;characterEncode=UTF-8");
		//config.setUsername("sneak");
		//config.setPassword("skst");
		config.setMaxConnectionAgeInSeconds(10);
		  //设置每个分区中的最大连接数 30
		config.setMaxConnectionsPerPartition(3);
      //设置每个分区中的最小连接数 10
		config.setMinConnectionsPerPartition(1);
        //当连接池中的连接耗尽的时候 BoneCP一次同时获取的连接数
        config.setAcquireIncrement(5);
        //连接释放处理
        config.setReleaseHelperThreads(3);
        //设置分区  分区数为3
        config.setPartitionCount(2);
        config.setCloseConnectionWatch(true);
		
        cpools = new BoneCPDataSource(config);
		
        Connection conn = cpools.getConnection();
        conn.setAutoCommit(true);
        ResultSet rs = conn.createStatement().executeQuery("select * from sneak_test");
	    if (rs.next()) {
			System.out.println(rs.getLong(1)+" ->"+rs.getString(2));
		}
        
	    conn.close();
		
	    Connection temp = cpools.getConnection();
	    temp.setReadOnly(true);
	    
	    
	    Statement st = temp.createStatement();
	    
	    rs =st.executeQuery("select * from sneak_test");
	    
	    
	    temp.close();
	    
	    
	    
	   /* Properties props = new Properties();

	    // We want this for failover on the slaves
	    props.put("autoReconnect", "true");

	    // We want to load balance between the slaves
	    props.put("roundRobinLoadBalance", "true");

	    props.put("user", "skst");
	    props.put("password", "TestDBSkst$@");

	    //
	    // Looks like a normal MySQL JDBC url, with a
	    // comma-separated list of hosts, the first
	    // being the 'master', the rest being any number
	    // of slaves that the driver will load balance against
	    //

	    Connection conn =
	        driver.connect("jdbc:mysql:replication://192.168.1.15,192.168.1.15/v506_20130109?useUnicode=true&amp;characterEncode=UTF-8",
	            props);

	    //
	    // Perform read/write work on the master
	    // by setting the read-only flag to "false"
	    //

	    conn.setReadOnly(true);
	    conn.setAutoCommit(false);
	    conn.createStatement().executeQuery("select * from account where accountId =1730");
	    conn.commit();
*/
	    //
	    // Now, do a query from a slave, the driver automatically picks one
	    // from the list
	    //

	   /* conn.setReadOnly(true);

	    ResultSet rs =
	      conn.createStatement().executeQuery("SELECT a,b FROM alt_table");*/
		
		
	
		
	}
	
}
