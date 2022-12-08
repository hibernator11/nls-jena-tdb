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

public class JenaTDBQueryTest {
    private static Logger logger = LoggerFactory.getLogger(JenaTDBQueryTest.class);
    // Why This Failure marker
    private static final Marker WTF_MARKER = MarkerFactory.getMarker("WTF");

    public static void main(String[] args) {
        try {
            // Create dataset
            Path path = Paths.get(".").toAbsolutePath().normalize();
            String dbDir = path.toFile().getAbsolutePath() + "/dbtest/";
            Location location = Location.create(dbDir);
            Dataset dataset = TDB2Factory.connectDataset(location);

            // create transaction for reading
            dataset.begin(ReadWrite.READ);
            QueryExecution qe = QueryExecutionFactory.create("SELECT distinct ?type " +
                                                            "WHERE {?s a ?type } LIMIT 5", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String strValue = qs.get("?type").toString();
                logger.info("Class = " + strValue);
            }

            qe = QueryExecutionFactory.create("SELECT (COUNT(distinct ?type) AS ?total) " +
                    "WHERE {?s a ?type }", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String strValue = qs.get("?total").toString();
                logger.info("Number of classes = " + strValue);
            }

            qe = QueryExecutionFactory.create("SELECT distinct ?p " +
                    "WHERE {?s ?p ?o } LIMIT 10", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String strValue = qs.get("?p").toString();
                logger.info("Property = " + strValue);
            }

            qe = QueryExecutionFactory.create("SELECT (COUNT(distinct ?p) AS ?total) " +
                    "WHERE {?s ?p ?o }", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String strValue = qs.get("?total").toString();
                logger.info("Number of properties = " + strValue);
            }

            // Releasing dataset resources
            dataset.close();
        } catch (Throwable t) {
            logger.error(WTF_MARKER, t.getMessage(), t);
        }
    }
}


