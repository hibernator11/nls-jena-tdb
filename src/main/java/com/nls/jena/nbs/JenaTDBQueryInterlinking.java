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

public class JenaTDBQueryInterlinking {
    private static Logger logger = LoggerFactory.getLogger(JenaTDBQueryInterlinking.class);
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
                            "SELECT (count(distinct ?s) as ?total) " +
                            "WHERE {" +
                            "?s a bf:Work ." +
                            "}", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String strValue = qs.get("?total").toString();
                logger.info("works total = " + strValue);
            }

            qe = QueryExecutionFactory.create(
                    "PREFIX bf:<http://id.loc.gov/ontologies/bibframe/> " +
                    "SELECT (count(distinct ?s) as ?total) " +
                    "WHERE {" +
                    "?s a bf:Work ." +
                    "?s bf:contribution ?c . ?c bf:role ?role ." +
                    " filter regex(str(?role), \"relators\") " +
                    "}", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String strValue = qs.get("?total").toString();
                logger.info("works linked to relators = " + strValue);
            }

            qe = QueryExecutionFactory.create(
                    "PREFIX bf:<http://id.loc.gov/ontologies/bibframe/> " +
                            "SELECT (count(distinct ?s) as ?total) " +
                            "WHERE {" +
                            "?s a bf:Work ." +
                            " ?s bf:hasInstance ?i . ?i bf:provisionActivity ?a ." +
                            "?a bf:place ?p . filter regex(str(?p), \"countries\")" +
                    "}", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String strValue = qs.get("?total").toString();
                logger.info("works linked to countries = " + strValue);
            }


            // Releasing dataset resources
            dataset.close();
        } catch (Throwable t) {
            logger.error(WTF_MARKER, t.getMessage(), t);
        }
    }
}


