package com.lifeix.test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;

import com.lifeix.db.DBConnectionManager;
import com.lifeix.db.DBWriteConnection;
import com.lifeix.db.IntfDBConnection;


public class DBtest {

	public static void main(String[] args) throws SQLException, FileNotFoundException, IOException {
		
		DBConnectionManager.getInstance().initDBpool(null);
		
		
		for (int i = 0; i < 100; i++) {
			IntfDBConnection conn = new DBWriteConnection("lifeix_sneak");
			//conn.queryByPreparedStatement("select * from sneak_test");
			conn.insertAndGetPrimarykeyByPreparedStatement("insert into sneak_test (name,`desc`) values (?,?)","第"+i,"高手");
			conn.release();
		}
		
		
		
		
		/*IntfDBConnection temp = new DBReadConnection("l06");
		temp.queryByPreparedStatement("select * from sneak_test");
		temp.release();*/
		
	}
}
