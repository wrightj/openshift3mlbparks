package org.openshift.mlbparks.postgresql;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

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
	private static Logger logger = Logger.getLogger(DBPostgreSQLConnection.class.getCanonicalName());
	private Connection conn;

	@PostConstruct
	public void afterCreate() {
		String postgresqlHost = System.getenv("POSTGRESQL_SERVICE_HOST");
		String postgresqlPort = System.getenv("POSTGRESQL_SERVICE_PORT");
		String postgresqlUser = System.getenv("POSTGRESQL_USER");
		String postgresqlPassword = System.getenv("POSTGRESQL_PASSWORD");
		String postgresqlDBName = System.getenv("POSTGRESQL_DATABASE");
		// Check if we are using a postgres template or postgres RHEL 7 image
		if (postgresqlHost == null) {
			postgresqlHost = System.getenv("POSTGRESQL_92_RHEL7_SERVICE_HOST");
		} 
		if (postgresqlPort == null) {
			postgresqlPort = System.getenv("POSTGRESQL_92_RHEL7_SERVICE_PORT");
		}
		boolean connected=false;
		try {
			Class.forName("org.postgresql.Driver");
			String url = "jdbc:postgresql://" + postgresqlHost + ":" + postgresqlPort + "/" + postgresqlDBName;
			conn = DriverManager.getConnection(url, postgresqlUser, postgresqlPassword);
			logger.log(Level.INFO,"Connected to database");
			connected=true;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			logger.log(Level.SEVERE,"Couldn't load the driver class: " + e.getMessage() + " :: " + e.getClass());

		} catch (SQLException e) {
			logger.log(Level.SEVERE,"Couldn't connect to PostgreSQL: " + e.getMessage() + " :: " + e.getClass());
		}
		
		if(connected)
			initDatabase();

	}
	
	private void initDatabase() {
		int teamsImported = 0;
		if (conn!=null) {
			logger.log(Level.SEVERE,"The database is empty.  We need to populate it");
			JSONParser jsonParser = new JSONParser();
			PreparedStatement st = null;
			try {
				
				String currentLine = new String();
				URL jsonFile = new URL(
						"https://raw.githubusercontent.com/securepaas/openshift3mlbparks/master/input.txt");
				BufferedReader in = new BufferedReader(new InputStreamReader(jsonFile.openStream()));
			
				st=conn.prepareStatement("DROP TABLE IF EXISTS mlbparks");
				st.executeUpdate();
				st.close();
				
				st=conn.prepareStatement("create table mlbparks ("
						+ "id	    SERIAL UNIQUE,"
						+ "name		VARCHAR(255),"
						+ "payroll  bigint,"
						+ "ballpark VARCHAR(255),"
						+ "league   VARCHAR(255),"
						+ "lat      double precision,"
						+ "long     double precision"
						+ ")");
				st.executeUpdate();
				st.close();
				while ((currentLine = in.readLine()) != null) {
					JSONObject jsonObject = (JSONObject) jsonParser.parse(currentLine);
					String name=jsonObject.get("name").toString();
					String ballpark=jsonObject.get("ballpark").toString();
					String league=jsonObject.get("league").toString();
					long payroll=(long)jsonObject.get("payroll");
					double latval,longval;
					
					JSONArray coords= (JSONArray) jsonObject.get("coordinates");
					@SuppressWarnings("unchecked")
					Iterator<Double> iterator = coords.iterator();
					longval=iterator.next();
					latval=iterator.next();
					
					st=conn.prepareStatement("insert into mlbparks (name,lat,long,ballpark,league,payroll) values "
							+ "(?,?,?,?,?,?)");
					st.setString(1, name);
					st.setDouble(2, latval);
					st.setDouble(3, longval);
					st.setString(4, ballpark);
					st.setString(5, league);
					st.setLong(6, payroll);
					st.executeUpdate();
					st.close();
					teamsImported++;
					
				}
				logger.log(Level.INFO,"Successfully imported " + teamsImported + " teams.");

			} catch (Exception e) {
				logger.log(Level.SEVERE,"Failed to initialize database: "+e.getMessage(),e);
			} finally {
				try {
					st.close();
				} catch (Exception e) {}
			}
		} else
			logger.log(Level.INFO,"The database is already configured.");
	}

	public Connection getConnection() throws Exception {
		if (conn != null) {
			return conn;
		} else {
			throw new Exception("Could not get a connection to the DB");
		}
	}

	public ArrayList<MLBPark> getMLBParks() {
		return getMLBParks(false,0,0,0,0);

	}
	
	public ArrayList<MLBPark> getMLBParksWithRange(float lat1,float lon1, float lat2,float lon2)
	{
		return getMLBParks(true,lat1, lon1, lat2, lon2);
	}
	
	private ArrayList<MLBPark> getMLBParks(boolean range,float lat1,float lon1, float lat2,float lon2)
	{
		ArrayList<MLBPark> result = new ArrayList<MLBPark>();
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			String statement="SELECT id,name,lat,long,ballpark,league,payroll "
					+ "FROM mlbparks ";
			if(range)
				statement=statement+"where lat <=? AND lat >=? AND long <=? AND long >=? ";
			
			statement=statement+ "ORDER BY id";
			
			st = conn.prepareStatement(statement);
			
			if(range)
			{
				logger.log(Level.INFO,"North lat is: "+lat1);
				logger.log(Level.INFO,"South lat is: "+lat2);
				logger.log(Level.INFO,"East long is: "+lon1);
				logger.log(Level.INFO,"West long is: "+lon2);
				st.setDouble(1, lat1);
				st.setDouble(2, lat2);
				st.setDouble(3, lon1);
				st.setDouble(4, lon2);
			}		
			rs = st.executeQuery();		
			while (rs.next()) {
				MLBPark mlbpark = new MLBPark();
				mlbpark.setId(rs.getString("id"));
				mlbpark.setName(rs.getString("name"));
				
				//TODO Fix this
				double longval=rs.getDouble("long");
				double latval=rs.getDouble("lat");
				
				mlbpark.setPosition(processCoordinates(longval,latval));
				mlbpark.setBallpark(rs.getString("ballpark"));
				mlbpark.setLeague(rs.getString("league"));
				mlbpark.setPayroll(rs.getLong("payroll"));

				result.add(mlbpark);
			}
		} catch (Exception se) {
			logger.log(Level.SEVERE,"Threw an Exception getting the list MLB Parks.");
			logger.log(Level.SEVERE,se.getMessage());
		} finally {
			try {
				rs.close();
				st.close();
			} catch (Exception e) {}
		}
		return result;
	}
	
	private List<Double> processCoordinates(double longval,double latval){
		List<Double> c = new ArrayList<Double>();
		c.add(Double.valueOf(longval));
		c.add(Double.valueOf(latval));
		return c;
				
	}
	
	//TODO on postgresql the coord record should be persisted like it's shown below 
	public static void main(String []args){
		DBPostgreSQLConnection c = new DBPostgreSQLConnection();
		List<Double> t= c.processCoordinates(-84.38839,33.734708);
		logger.log(Level.INFO,"Result"+t);
	}
}
