package com.example.demo;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.springframework.stereotype.Service;

@Service
@SpringBootApplication
public class DemoApplication {

	static Connection jdbcConncetion = null;
	static PreparedStatement jdbcPrepareStat = null;
	static InputStream inputStream;

	public static void main(String[] args)
			throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException {

		try {
			
			//collection of system arguments

			String db_name = args[0];
			String table_name = args[1];
			String filename = table_name + ".csv";

			log("Starting Assignment 1");
			makeJDBCConnection(db_name);

			log("Let's get Data from DB");

			log("Writing Data to CSV file Complated");

			Read_and_Write(table_name, filename);
			

		} catch (Exception e) {

			e.printStackTrace();
		}

	}

	//making JDBC Connection
	
	private static void makeJDBCConnection(String db_name) {

		try {
			Class.forName("com.mysql.jdbc.Driver");
			log("Congrats - Seems your MySQL JDBC Driver Registered!");
		} catch (ClassNotFoundException e) {
			log("Sorry, couldn't found JDBC driver. Make sure you have added JDBC Maven Dependency Correctly");
			e.printStackTrace();
			return;
		}

		try {

			jdbcConncetion = DriverManager.getConnection("jdbc:mysql://localhost:3306/" + db_name, "root", "");
			if (jdbcConncetion != null) {
				log("Connection Successful! Enjoy. Now it's time to push data");
			} else {
				log("Failed to make connection!");
			}
		} catch (SQLException e) {
			log("MySQL Connection Failed!");
			e.printStackTrace();
			return;
		}
	}
	
	//Read Data From MYSQL and Writing to CSV File.
	
	private static void Read_and_Write(String table_name,String filename)
	{

		try
		{
		log("Writing Data to CSV file");

		//Dynamic Query Parse..
		String query = "select * from " + table_name;
		
		FileWriter fw = new FileWriter(filename);
		java.sql.Statement stmt = jdbcConncetion.createStatement();

		//ResultSet for columns data and ResultSetMetaData for columns name
		ResultSet rs = stmt.executeQuery(query);
		ResultSetMetaData rsmd = rs.getMetaData();
		
		//temp holder for data collection in string 
		int Total_Columns = 1;
		String temp_holder = "";
		
		//Getting all the columns Names...
		try {

			while (true) {
				temp_holder += rsmd.getColumnName(Total_Columns) + ",";
				Total_Columns += 1;
			}

		} catch (Exception e) {
			temp_holder = (temp_holder.substring(0, temp_holder.length() - 1)) + "\n";
			fw.append(temp_holder);
		}

		log("Column data complate");
	
	//Writing data to csv File...
		
		while (rs.next()) {

			temp_holder = "";
			
			for(int i=1;i<Total_Columns;i++)
			{
	
				temp_holder += String.valueOf(rs.getString(i))+",";
				
			}
		
			temp_holder = (temp_holder.substring(0, temp_holder.length() - 1)) + "\n";
			fw.append(temp_holder);
			
		}

		fw.flush();
		fw.close();
			jdbcPrepareStat.close();
			jdbcConncetion.close(); // connection close

			log("JDBC Connection Closed");

}
catch(Exception e)
{

}

	}
	
	// Simple log utility
	private static void log(String string) {
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm:ss");
		LocalDateTime now = LocalDateTime.now();

		System.out.println(dtf.format(now) + "	" + string);

	}
}

