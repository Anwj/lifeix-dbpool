package com.lifeix.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.jdbc.NonRegisteringDriver;

/**
 * @author ahuaxuan(aaron zhang)
 * @since 2008-6-18
 * @version $Id$
 */
public class EasyNonRegisteringReplicationDriver extends NonRegisteringDriver {

	private static transient Logger logger = LoggerFactory.getLogger(EasyNonRegisteringReplicationDriver.class);
	public EasyNonRegisteringReplicationDriver() throws SQLException {
	}

	public Connection connect(String url, Properties info) throws SQLException {
		Properties parsedProps = parseURL(url, info);
		if (parsedProps == null) {
			return null;
		}
		Properties masterProps = (Properties) parsedProps.clone();
		Properties slavesProps = (Properties) parsedProps.clone();
		slavesProps.setProperty("com.mysql.jdbc.ReplicationConnection.isSlave",
				"true");
		
		String hostValues = parsedProps.getProperty("HOST");
		
		if (hostValues != null) {
			StringTokenizer st = new StringTokenizer(hostValues, ",");
			StringBuffer masterHost = new StringBuffer();
			StringBuffer slaveHosts = new StringBuffer();
			if (st.hasMoreTokens()) {
				String hostPortPair[] = parseHostPortPair(st.nextToken());
				if (hostPortPair[0] != null) {
					masterHost.append(hostPortPair[0]);
				}
				if (hostPortPair[1] != null) {
					masterHost.append(":");
					masterHost.append(hostPortPair[1]);
				}
			}
			boolean firstSlaveHost = true;
			do {
				if (!st.hasMoreTokens()) {
					break;
				}
				String hostPortPair[] = parseHostPortPair(st.nextToken());
				if (!firstSlaveHost) {
					slaveHosts.append(",");
				} else {
					firstSlaveHost = false;
				}
				if (hostPortPair[0] != null) {
					slaveHosts.append(hostPortPair[0]);
				}
				if (hostPortPair[1] != null) {
					slaveHosts.append(":");
					slaveHosts.append(hostPortPair[1]);
				}
			} while (true);
			if (slaveHosts.length() == 0) {
				if (logger.isWarnEnabled()) {
					logger.warn("------- There is no slaves configurations --------");
				}
			}
			masterProps.setProperty("HOST", masterHost.toString());
			slavesProps.setProperty("HOST", slaveHosts.toString());
		}
		return new EasyReplicationConnection(masterProps, slavesProps);
	}
}
