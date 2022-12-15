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

public class JenaTDBQueryAccuracy {
    private static Logger logger = LoggerFactory.getLogger(JenaTDBQueryAccuracy.class);
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

            QueryExecution qe = QueryExecutionFactory.create(
                    "PREFIX bf:<http://id.loc.gov/ontologies/bibframe/> " +
                            "PREFIX bflc:<http://id.loc.gov/ontologies/bflc/> " +
                            "SELECT distinct ?date " +
                            "WHERE {" +
                            "?s bflc:simpleDate ?date ." +
                            "} order by ?date limit 200 ", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String strValue = qs.get("?date").toString();
                logger.info("date = " + strValue);
            }

            qe = QueryExecutionFactory.create("PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#> " +
                    "SELECT distinct ?s " +
                    "WHERE {?s ?p ?r. ?r a <http://id.loc.gov/ontologies/bibframe/Role> . ?r rdfs:label ?l . " +
                    "filter regex(?l, \"auhtor\") } " +
                    "limit 100", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String lValue = qs.get("?s").toString();

                logger.info("Roles:" + lValue);
            }

            // Releasing dataset resources
            dataset.close();
        } catch (Throwable t) {
            logger.error(WTF_MARKER, t.getMessage(), t);
        }
    }
}


