package edu.upenn.cis.cis455.storage;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public abstract class RDSStorage {

	final String DB_NAME = "postgres";
	final String USERNAME = System.getenv("RDS_USERNAME");
	final String PASSWORD = System.getenv("RDS_PASSWORD");
	final int PORT = 5432;
	final String HOSTNAME = System.getenv("RDS_HOSTNAME");
	
	public Connection getDBConnection() throws SQLException {
		String jdbcUrl = "jdbc:postgresql://" + HOSTNAME + ":" + PORT + "/" + 
				DB_NAME + "?user=" + USERNAME + "&password=" + PASSWORD;
		
	    return DriverManager.getConnection(jdbcUrl);
	}
}
