package org.openshift.mlbparks.postgresql;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Named;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
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
		boolean connected=false;
		try {
			Class.forName("org.postgresql.Driver");
			String url = "jdbc:postgresql://" + postgresqlHost + ":" + postgresqlPort + "/" + postgresqlDBName;
			conn = DriverManager.getConnection(url, postgresqlUser, postgresqlPassword);
			System.out.println("Connected to database");
			connected=true;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.out.println("Couldn't load the driver class: " + e.getMessage() + " :: " + e.getClass());

		} catch (SQLException e) {
			System.out.println("Couldn't connect to PostgreSQL: " + e.getMessage() + " :: " + e.getClass());
		}
		
		if(connected)
			initDatabase();

	}
	
	private void initDatabase() {
		int teamsImported = 0;
		if (conn!=null && getMLBParks().isEmpty()) {
			System.out.println("The database is empty.  We need to populate it");
			JSONParser jsonParser = new JSONParser();
			PreparedStatement st = null;
			ResultSet rs = null;
			try {
				
				String currentLine = new String();
				URL jsonFile = new URL(
						"https://raw.githubusercontent.com/securepaas/openshift3mlbparks/master/input.txt");
				BufferedReader in = new BufferedReader(new InputStreamReader(jsonFile.openStream()));
			
				st=conn.prepareStatement("create table mlbparks ("
						+ "id	    SERIAL UNIQUE,"
						+ "name		VARCHAR(255),"
						+ "payroll  int2,"
						+ "ballpark VARCHAR(255),"
						+ "league   VARCHAR(255),"
						+ "lat      double precision,"
						+ "long     double precision"
						+ ")");
				boolean tableMade=st.execute();
				if(!tableMade)
					throw new Exception("Failed to make table");
				while ((currentLine = in.readLine()) != null) {
					JSONObject jsonObject = (JSONObject) jsonParser.parse(currentLine);
					String name=jsonObject.get("name").toString();
					String ballpark=jsonObject.get("ballpark").toString();
					String league=jsonObject.get("league").toString();
					String payroll=jsonObject.get("payroll").toString();
					String latval,longval;
					
					JSONArray coords= (JSONArray) jsonObject.get("coordinates");
					@SuppressWarnings("unchecked")
					Iterator<String> iterator = coords.iterator();
					longval=iterator.next();
					latval=iterator.next();
					
					st=conn.prepareStatement("insert into mlbparks (name,lat,long,ballpark,league,payroll) values "
							+ "(?,?,?,?,?,?)");
					st.setString(2, name);
					st.setString(3, latval);
					st.setString(4, longval);
					st.setString(5, ballpark);
					st.setString(6, league);
					st.setString(7, payroll);
					rs=st.executeQuery();
					rs.close();
					st.close();
					teamsImported++;
				}
				System.out.println("Successfully imported " + teamsImported + " teams.");

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					rs.close();
					st.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
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
			rs = st.executeQuery("SELECT id,name,lat,long,ballpark,league,payroll FROM mlbparks ORDER BY id");

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
	
	public ArrayList<MLBPark> getMLBParksWithRange(float lat1,float lon1, float lat2,float lon2)
	{
		ArrayList<MLBPark> result = new ArrayList<MLBPark>();
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			st = conn.prepareStatement("SELECT id,name,lat,long,ballpark,league,payroll "
					+ "FROM mlbparks "
					+ "where lat <=? AND lat >=? AND "
					+ "long <=? AND long >=? "
					+ "ORDER BY id");
			st.setDouble(1, lat1);
			st.setDouble(2, lat2);
			st.setDouble(3, lon1);
			st.setDouble(4, lon2);
			
			System.out.println("North lat is: "+lat1);
			System.out.println("South lat is: "+lat2);
			System.out.println("East long is: "+lon1);
			System.out.println("West long is: "+lon2);
			
			rs = st.executeQuery();
			
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
		c.add(Double.valueOf(coordinates[0]));
		c.add(Double.valueOf(coordinates[1]));
		result.put("coordinates",c);
		return result;
		
		
	}
	
	
	//TODO on postgresql the coord record should be persisted like it's shown below 
	public static void main(String []args){
		DBPostgreSQLConnection c = new DBPostgreSQLConnection();
		Map<String,List<Double>> t= c.processCoordinates("coordinates:-84.38839:33.734708");
		System.out.println("Result"+t);
	}
}
