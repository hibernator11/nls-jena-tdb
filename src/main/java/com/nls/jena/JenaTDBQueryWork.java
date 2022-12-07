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

public class JenaTDBQueryWork {
    private static Logger logger = LoggerFactory.getLogger(JenaTDBQueryWork.class);
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
            QueryExecution qe = QueryExecutionFactory.create("SELECT (COUNT(distinct ?s) AS ?total) " +
                    "WHERE {?s a <http://id.loc.gov/ontologies/bibframe/Work> }", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String strValue = qs.get("?total").toString();
                logger.info("number of Works = " + strValue);
            }

            qe = QueryExecutionFactory.create("SELECT ?p ?o " +
                    "WHERE {<http://example.org/99116414544204341#Work> ?p ?o}", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String pValue = qs.get("?p").toString();
                String oValue = qs.get("?o").toString();
                logger.info("http://example.org/99116414544204341#Work pValue:" + pValue + " oValue:" + oValue);
            }

            qe = QueryExecutionFactory.create("SELECT ?w (count(?i) as ?instances) " +
                    "WHERE {?w <http://id.loc.gov/ontologies/bibframe/hasInstance> ?i}" +
                    "group by ?w " +
                    "having (count(?i)>1) limit 10", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String wValue = qs.get("?w").toString();
                String iValue = qs.get("?instances").toString();
                logger.info("instances w:" + wValue + " iValue:" + iValue);
            }

            qe = QueryExecutionFactory.create("SELECT ?i " +
                    "WHERE {<http://example.org/9940428573804341#Work> <http://id.loc.gov/ontologies/bibframe/hasInstance> ?i}", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String iValue = qs.get("?i").toString();
                logger.info("instances:" + " iValue:" + iValue);
            }

            qe = QueryExecutionFactory.create("SELECT ?p ?o " +
                    "WHERE {<http://example.org/9940428573804341#Instance856-17> ?p ?o}", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String pValue = qs.get("?p").toString();
                String iValue = qs.get("?o").toString();
                logger.info("http://example.org/9940428573804341#Instance856-17:" + pValue + " iValue:" + iValue);
            }

            qe = QueryExecutionFactory.create("SELECT ?p ?o " +
                    "WHERE {<http://example.org/9940428573804341#Instance> ?p ?o}", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String pValue = qs.get("?p").toString();
                String iValue = qs.get("?o").toString();
                logger.info("http://example.org/9940428573804341#Instance:" + pValue + " iValue:" + iValue);
            }

            // Releasing dataset resources
            dataset.close();
        } catch (Throwable t) {
            logger.error(WTF_MARKER, t.getMessage(), t);
        }
    }
}

