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

public class JenaTDBQueryInstance {
    private static Logger logger = LoggerFactory.getLogger(JenaTDBQueryInstance.class);
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
            QueryExecution qe = QueryExecutionFactory.create("SELECT ?p ?o " +
                    "WHERE {<http://example.org/9930877183804341#Instance> ?p ?o}", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String pValue = qs.get("?p").toString();
                String oValue = qs.get("?o").toString();
                logger.info("http://example.org/9930877183804341#Instance pValue:" + pValue + " oValue:" + oValue);
            }

            // Releasing dataset resources
            dataset.close();
        } catch (Throwable t) {
            logger.error(WTF_MARKER, t.getMessage(), t);
        }
    }
}

