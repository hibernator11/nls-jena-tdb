package com.nls.jena.general;

import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.query.*;
import org.apache.jena.tdb2.TDB2Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

public class JenaTDBQuery {
    private static Logger logger = LoggerFactory.getLogger(JenaTDBQuery.class);
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
            QueryExecution qe = QueryExecutionFactory.create("SELECT distinct ?type " +
                                                            "WHERE {?s a ?type }", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String strValue = qs.get("?type").toString();
                logger.info("value = " + strValue);
            }

            qe = QueryExecutionFactory.create("SELECT (COUNT(distinct ?type) AS ?total) " +
                    "WHERE {?s a ?type }", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String strValue = qs.get("?total").toString();
                logger.info("number of classes = " + strValue);
            }

            qe = QueryExecutionFactory.create("SELECT distinct ?p " +
                    "WHERE {?s ?p ?o }", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String strValue = qs.get("?p").toString();
                logger.info("value = " + strValue);
            }

            qe = QueryExecutionFactory.create("SELECT (COUNT(distinct ?p) AS ?total) " +
                    "WHERE {?s ?p ?o }", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String strValue = qs.get("?total").toString();
                logger.info("number of properties = " + strValue);
            }

            qe = QueryExecutionFactory.create("SELECT (COUNT(*) AS ?total) " +
                    "WHERE {?s ?p ?o }", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String strValue = qs.get("?total").toString();
                logger.info("number of triples = " + strValue);
            }

            qe = QueryExecutionFactory.create("SELECT (COUNT(distinct ?o) AS ?total) " +
                    "WHERE {?s ?p ?o . filter regex(str(?o), \"vocabulary\")}", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String strValue = qs.get("?total").toString();
                logger.info("number of external links = " + strValue);
            }

            qe = QueryExecutionFactory.create("SELECT distinct ?o " +
                    "WHERE {?s ?p ?o . filter regex(str(?o), \"vocabulary\")} limit 10", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String strValue = qs.get("?o").toString();
                logger.info("example of external links = " + strValue);
            }

            // Releasing dataset resources
            dataset.close();
        } catch (Throwable t) {
            logger.error(WTF_MARKER, t.getMessage(), t);
        }
    }
}


