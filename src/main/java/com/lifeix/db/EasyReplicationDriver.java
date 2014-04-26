package com.lifeix.db;


import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

// Referenced classes of package com.mysql.jdbc:
//            NonRegisteringReplicationDriver
/**
 * @author ahuaxuan(aaron zhang)
 * @since 2008-6-18
 * @version $Id$
 */
public class EasyReplicationDriver extends EasyNonRegisteringReplicationDriver
		implements Driver {

	public EasyReplicationDriver() throws SQLException {
	}

	static {
		try {
			DriverManager.registerDriver(new EasyNonRegisteringReplicationDriver());
		} catch (SQLException E) {
			throw new RuntimeException("Can't register driver!");
		}
	}
}
