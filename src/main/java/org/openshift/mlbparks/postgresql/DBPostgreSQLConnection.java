package org.openshift.mlbparks.postgresql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Named;

import org.openshift.mlbparks.domain.MLBPark;

@Named
@ApplicationScoped
public class DBPostgreSQLConnection {
	private Connection conn;

	@PostConstruct
	public void afterCreate() {
		String postgresqlHost = System.getenv("POSTGRESQL_SERVICE_HOST");
		String postgresqlPort = System.getenv("POSTGRESQL_SERVICE_PORT");
		String postgresqlUser = System.getenv("POSTGRESQL_USER");
		String postgresqlPassword = System.getenv("POSTGRESQL_PASSWORD");
		String postgresqlDBName = System.getenv("POSTGRESQL_DATABASE");
		try {
			Class.forName("org.postgresql.Driver");
			String url = "jdbc:postgresql://" + postgresqlHost + ":" + postgresqlPort + "/" + postgresqlDBName;
			conn = DriverManager.getConnection(url, postgresqlUser, postgresqlPassword);
			System.out.println("Connected to database");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.out.println("Couldn't load the driver class: " + e.getMessage() + " :: " + e.getClass());

		} catch (SQLException e) {
			System.out.println("Couldn't connect to PostgreSQL: " + e.getMessage() + " :: " + e.getClass());
		}

	}

	public Connection getConnection() throws Exception {
		if (conn != null) {
			return conn;
		} else {
			throw new Exception("Could not get a connection to the DB");
		}
	}

	public ArrayList<MLBPark> getMLBParks() {
		ArrayList<MLBPark> result = new ArrayList<MLBPark>();
		Statement st = null;
		ResultSet rs = null;
		try {
			st = conn.createStatement();
			rs = st.executeQuery("SELECT id,name,coordinates,ballpark,league,payroll FROM mlbparks ORDER BY id");

			while (rs.next()) {
				MLBPark mlbpark = new MLBPark();
				mlbpark.setId(rs.getString("id"));
				mlbpark.setName(rs.getString("name"));
				mlbpark.setPosition(this.processCoordinates(rs.getString("coordinates")));
				mlbpark.setBallpark(rs.getString("ballpark"));
				mlbpark.setLeague(rs.getString("league"));
				mlbpark.setPayroll(rs.getString("payroll"));

				result.add(mlbpark);
			}
		} catch (SQLException se) {
			System.err.println("Threw a SQLException creating the list of blogs.");
			System.err.println(se.getMessage());
		} finally {
			try {
				rs.close();
				st.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return result;

	}
	private Map<String,List<Double>> processCoordinates(String coord){
		Map<String,List<Double>> result = new HashMap<String,List<Double>>();
		String[] coordinates= coord.split(":");
		for (int i = 0; i < coordinates.length; i++) {
			System.out.println(coordinates[i]);
		}
		List<Double> c = new ArrayList<Double>();
		c.add(Double.valueOf(coordinates[1]));
		c.add(Double.valueOf(coordinates[1]));
		result.put(coordinates[0],c);
		return result;
		
		
	}
	
	
	//TODO on postgresql the coord record should be persisted like it's shown below 
	public static void main(String []args){
		DBPostgreSQLConnection c = new DBPostgreSQLConnection();
		Map<String,List<Double>> t= c.processCoordinates("coordinates:-84.38839:33.734708");
		System.out.println("Result"+t);
	}
}
