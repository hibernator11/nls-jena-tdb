package com.nls.jena;

import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.query.*;
import org.apache.jena.tdb2.TDB2Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

public class JenaTDBQueryAgentRoles {
    private static Logger logger = LoggerFactory.getLogger(JenaTDBQueryAgentRoles.class);
    // Why This Failure marker
    private static final Marker WTF_MARKER = MarkerFactory.getMarker("WTF");

    public static void main(String[] args) {
        try {
            // Create dataset
            Path path = Paths.get(".").toAbsolutePath().normalize();
            String dbDir = path.toFile().getAbsolutePath() + "/db/";
            Location location = Location.create(dbDir);
            Dataset dataset = TDB2Factory.connectDataset(location);

            // create transaction for reading
            dataset.begin(ReadWrite.READ);
            QueryExecution qe = QueryExecutionFactory.create("PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#> " +
                    "SELECT * " +
                    "WHERE {?s a <http://id.loc.gov/ontologies/bibframe/Role> . ?s ?p ?o} limit 20", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String sValue = qs.get("?s").toString();
                String pValue = qs.get("?p").toString();
                String oValue = qs.get("?o").toString();

                logger.info("Role " + sValue + " p:" + pValue + " " + oValue);
            }

            qe = QueryExecutionFactory.create("PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#> " +
                    "SELECT distinct ?l " +
                    "WHERE {?s a <http://id.loc.gov/ontologies/bibframe/Role> . ?s rdfs:label ?l} limit 10", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String lValue = qs.get("?l").toString();

                logger.info("Roles:" + lValue);
            }

            qe = QueryExecutionFactory.create("PREFIX bf:<http://id.loc.gov/ontologies/bibframe/> " +
                    "SELECT distinct ?role " +
                    "WHERE {?s a bf:Work . ?s bf:contribution ?c . " +
                    " ?c bf:role ?role ." +
                    " filter (regex(str(?role), \"relator\"))" +
                    "} limit 20", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String roleValue = qs.get("?role").toString();

                logger.info("loc relators:" + roleValue);
            }

            qe = QueryExecutionFactory.create("PREFIX bf:<http://id.loc.gov/ontologies/bibframe/> " +
                    "SELECT (count(distinct ?role) as ?total) " +
                    "WHERE {?s a bf:Work . ?s bf:contribution ?c . " +
                    " ?c bf:role ?role ." +
                    " filter (regex(str(?role), \"relator\"))" +
                    "} limit 10", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String roleValue = qs.get("?total").toString();

                logger.info("loc relators total:" + roleValue);
            }



            // Releasing dataset resources
            dataset.close();
        } catch (Throwable t) {
            logger.error(WTF_MARKER, t.getMessage(), t);
        }
    }
}

