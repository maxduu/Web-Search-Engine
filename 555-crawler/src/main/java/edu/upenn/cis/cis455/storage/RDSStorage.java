package edu.upenn.cis.cis455.storage;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public abstract class RDSStorage {

	final String DB_NAME = "postgres";
	final String USERNAME = "master";
	final String PASSWORD = "ilovezackives";
	final int PORT = 5432;
	final String HOSTNAME = "cis555-project.ckm3s06jrxk1.us-east-1.rds.amazonaws.com";
	
	public Connection getDBConnection() throws SQLException {
		String jdbcUrl = "jdbc:postgresql://" + HOSTNAME + ":" + PORT + "/" + 
				DB_NAME + "?user=" + USERNAME + "&password=" + PASSWORD;
		
	    return DriverManager.getConnection(jdbcUrl);
	}
}
