package org.morgan.test.MavenSparkApp;

import java.util.List;
import java.sql.*;

public class SQLTest {

	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static String url = "jdbc:mysql://10.153.7.114:3306/mydb";
	static String username = "root";
	static String password = "aspect2015";

	SQLTest() {
	
		Connection conn = null;
		Statement stmt = null;

		int user = 1;
		int product = 2;
		int rating = 3;
		int order = 4;

		try {
			Class.forName("com.mysql.jdbc.Driver");

			System.out.println("Connecting to Database");
			conn = DriverManager.getConnection(url, username, password);
			System.out.println("Successfully Connected");

			System.out.println("Creating Statement");
			stmt = conn.createStatement();
			String sql;
			String check = "SELECT user_id from test";
			System.out.println("Successfully Created Statement");
			ResultSet rs = stmt.executeQuery(check);
			if (!rs.first() && !rs.next()) {// If first and next records are
											// empty
				// INSERT VALUES INTO TABLE
				sql = "INSERT INTO test " + "VALUES(" + user + "," + product
						+ "," + rating + "," + order + ")";
				stmt.executeUpdate(sql);
				System.out.println("Successfully wrote to Database");

			} else {
				// UPDATE VALUES IN TABLE
				int product1 =7956;
				int rating1 = 2;
				sql = "update test set listing_id=?, rating=? where test.user_id=? AND test.order=?";
				PreparedStatement ps = conn.prepareStatement(sql);

				ps.setInt(1, product1);
				ps.setInt(2, rating1);
				ps.setInt(3, user);
				ps.setInt(4, order);

				 ps.executeUpdate();

				// sql = "UPDATE test"
				// + "SET user_id=" + user
				// + "SET listing_id="+ product2
				// + ", rating="+ rating3
				// + ", order=" + order
				// + " WHERE user_id=" +user
				// + ", order=" + order;
				// stmt.executeUpdate(sql);
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
