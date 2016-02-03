package org.openshift.mlbparks.rest;

import java.util.ArrayList;
import java.util.List;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import org.openshift.mlbparks.domain.MLBPark;
import org.openshift.mlbparks.postgresql.DBPostgreSQLConnection;

@RequestScoped
@Path("/parks")
public class MLBParkResource {
	@Inject
	private DBPostgreSQLConnection dbPostgreSQLConnection;

	@GET()
	@Produces("application/json")
	public List<MLBPark> getAllParks() {
		ArrayList<MLBPark> allParksList = new ArrayList<MLBPark>();
		allParksList = dbPostgreSQLConnection.getMLBParks();
		return allParksList;
	}

	@GET
	@Produces("application/json")
	@Path("within")
	public List<MLBPark> findParksWithin(@QueryParam("lat1") float lat1,
			@QueryParam("lon1") float lon1, @QueryParam("lat2") float lat2,
			@QueryParam("lon2") float lon2) {
		return dbPostgreSQLConnection.getMLBParksWithRange(lat1, lon1, lat2, lon2);
	}
}
