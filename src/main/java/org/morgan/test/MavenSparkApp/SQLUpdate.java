package org.morgan.test.MavenSparkApp;

import java.util.List;
import java.sql.*;

import org.apache.spark.mllib.recommendation.Rating;

public class SQLUpdate implements UpdateSQL {

	// JDBC driver name and database URL
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://10.153.7.114:3306/mydb";
	// Database Credentials
	static final String USER = "root";
	static final String PWD = "aspect2015";

	public  void UpdateSQL(List<Rating> sortedRating) {
		// TODO Auto-generated method stub

		for (int i = 0; i <= sortedRating.size(); i++) {
			int user = sortedRating.get(i).user();
			double rating = sortedRating.get(i).rating();
			int product = sortedRating.get(i).product();
			int order = i;
			connectSQL(user, rating, product, order);
		}
	}

	public static void connectSQL(int user, double rating, int product,
			int order) {
		Connection conn = null;
		Statement stmt = null;
		try {
			// Register JDBC Driver
			try {
				Class.forName(JDBC_DRIVER);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("Connecting to Database");
			// Open a Connection
			conn = DriverManager.getConnection(DB_URL, USER, PWD);
			// Create a Statement
			System.out.println("Creating SQL Statement");

			stmt = conn.createStatement();
			String check;
			String sql;
			check = "SELECT user_id from recommendation";
			ResultSet rs = stmt.executeQuery(check);
			if (!rs.first() && !rs.next()) {// If first and next records are
											// empty
				// INSERT VALUES INTO TABLE
				sql = "INSERT INTO recommendation " + "VALUES(" + user + ","
						+ product + "," + rating + "," + order + ")";
				System.out.println("Successfully wrote to Database");
			} else {
				// UPDATE VALUES IN TABLE
				sql = "update test set listing_id=?, rating=? where test.user_id=? AND test.order=?";
				PreparedStatement ps = conn.prepareStatement(sql);

				ps.setInt(1, product);
				ps.setDouble(2, rating);
				ps.setInt(3, user);
				ps.setInt(4, order);

				ps.executeUpdate();

				System.out.println("Successfully Updated Table");
			}
			stmt.close();
			conn.close();
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se) {
				System.out.println("Why have you done this?");
			}
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
		System.out.println("Program Ending");
	}
	
}
