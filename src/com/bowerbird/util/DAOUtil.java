package com.bowerbird.util;

import java.sql.Connection;
import java.sql.DriverManager;

public class DAOUtil {
	public static Connection getConn(String dbname){
		Connection conn = null;
		try {
			if(dbname.equals("h2"))
			{
				Class.forName("org.h2.Driver");
				conn=DriverManager.getConnection("jdbc:h2:mem:fasterDB;DB_CLOSE_DELAY=-1");
			}
			else if(dbname.equals("monetdb"))
			{
				Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
				conn=DriverManager.getConnection("jdbc:monetdb://127.0.0.1:50000/demo", "monetdb", "monetdb");
			}
			

		} catch (Exception e) {
			e.printStackTrace();
		}  
		return conn;
	}
}
