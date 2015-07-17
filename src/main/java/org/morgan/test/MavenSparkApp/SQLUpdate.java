
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
			System.out.println("Creating Statement");
			stmt = conn.createStatement();

			sql = "INSERT INTO test VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE listing_id=?, rating=?";
			PreparedStatement statement = conn.prepareStatement(sql);

			statement.setInt(1, user);
			statement.setInt(2, product);
			statement.setDouble(3, rating);
			statement.setInt(4, order);
			statement.setInt(5, product);
			statement.setDouble(6, rating);

			statement.executeUpdate();

			System.out.println("Successfully wrote to Database");

			System.out.println("Successfully Updated Table");

			check = "SELECT * FROM test";
			System.out.println("Successfully Created Statement");
			ResultSet resultSet = stmt.executeQuery(check);
			writeResultSet(resultSet);
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

	private static void writeResultSet(ResultSet resultSet) throws SQLException {
		// TODO Auto-generated method stub
		System.out.println("Fetching Data");
		while (resultSet.next()) {
			int user = resultSet.getInt("user_id");
			int listing = resultSet.getInt("listing_id");
			int rating = resultSet.getInt("rating");
			int order = resultSet.getInt("order");
			System.out.println("User: " + user);
			System.out.println("Listing: " + listing);
			System.out.println("Rating: " + rating);
			System.out.println("Order: " + order);

		}
		resultSet.close();
	}
	
	
}
