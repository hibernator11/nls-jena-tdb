package com.nls.jena.nbs;

import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.query.*;
import org.apache.jena.tdb2.TDB2Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

public class JenaTDBQueryGeographicCoverage {
    private static Logger logger = LoggerFactory.getLogger(JenaTDBQueryGeographicCoverage.class);
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
            QueryExecution qe = QueryExecutionFactory.create("SELECT (count(distinct ?g) as ?total) " +
                    "WHERE {?s <http://id.loc.gov/ontologies/bibframe/geographicCoverage> ?g ." +
                    "filter regex(str(?g), \"geographic\") " +
                    "}", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String strValue = qs.get("?total").toString();
                logger.info("total Geographic coverage = " + strValue);
            }

            qe = QueryExecutionFactory.create("SELECT distinct ?g " +
                    "WHERE {?s <http://id.loc.gov/ontologies/bibframe/geographicCoverage> ?g ." +
                    "filter regex(str(?g), \"geographic\") " +
                    "}", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String strValue = qs.get("?g").toString();
                logger.info("Geographic coverage = " + strValue);
            }

            // Releasing dataset resources
            dataset.close();
        } catch (Throwable t) {
            logger.error(WTF_MARKER, t.getMessage(), t);
        }
    }
}

