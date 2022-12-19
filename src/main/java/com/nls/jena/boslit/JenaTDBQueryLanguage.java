package com.nls.jena.boslit;

import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.query.*;
import org.apache.jena.tdb2.TDB2Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

public class JenaTDBQueryLanguage {
    private static Logger logger = LoggerFactory.getLogger(JenaTDBQueryLanguage.class);
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
            QueryExecution qe = QueryExecutionFactory.create("SELECT distinct ?l " +
                    "WHERE {?s <http://id.loc.gov/ontologies/bibframe/language> ?l ." +
                    "filter regex(str(?l), \"http\") " +
                    "}", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String lValue = qs.get("?l").toString();
                logger.info("Languages lValue:" + lValue);
            }

            qe = QueryExecutionFactory.create("SELECT (count(distinct ?s) as ?total) " +
                    "WHERE {?s <http://id.loc.gov/ontologies/bibframe/language> ?l ." +
                    "filter regex(str(?l), \"http\") " +
                    "}", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String lValue = qs.get("?total").toString();
                logger.info("Works with languages total:" + lValue);
            }

            qe = QueryExecutionFactory.create("SELECT ?l (count(distinct ?s) as ?total) " +
                    "WHERE {?s <http://id.loc.gov/ontologies/bibframe/language> ?l ." +
                    "filter regex(str(?l), \"http\") " +
                    "} group by ?l order by desc(?total) limit 10", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String lValue = qs.get("?l").toString();
                logger.info("Languages lValue-total:" + lValue + " " + qs.get("?total").toString());
            }



            qe = QueryExecutionFactory.create("SELECT * " +
                    "WHERE {<http://example.org/16493#Hub240-9-Agent> ?p ?o ." +
                    "}", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String pValue = qs.get("?p").toString();
                String oValue = qs.get("?o").toString();
                logger.info("http://example.org/16493#Hub240-9-Agent:" + pValue + " " + oValue);
            }

            /*qe = QueryExecutionFactory.create("SELECT distinct ?s ?t " +
                    "WHERE {?s <http://id.loc.gov/ontologies/bibframe/language> <http://id.loc.gov/vocabulary/languages/spa> ." +
                    "       ?s <http://id.loc.gov/ontologies/bibframe/title> ?title ." +
                    "       ?title <http://id.loc.gov/ontologies/bibframe/mainTitle> ?t " +
                    "}", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String tValue = qs.get("?t").toString();
                String sValue = qs.get("?s").toString();
                logger.info("Works Spanish:" + sValue + " " + tValue);
            }*/

            // Releasing dataset resources
            dataset.close();
        } catch (Throwable t) {
            logger.error(WTF_MARKER, t.getMessage(), t);
        }
    }
}

