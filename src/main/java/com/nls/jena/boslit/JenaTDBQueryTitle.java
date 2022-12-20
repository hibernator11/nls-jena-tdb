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

public class JenaTDBQueryTitle {
    private static Logger logger = LoggerFactory.getLogger(JenaTDBQueryTitle.class);
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
                    "SELECT ?mt (COUNT(?w) as ?total) " +
                    "WHERE {" +
                    "  ?w bf:expressionOf ?e. " +
                    "  ?e bf:title ?t. " +
                    "  ?t bf:mainTitle ?mt" +
                    "} " +
                    " GROUP BY ?mt " +
                    " HAVING (?total>1) ORDER BY DESC(?total)" +
                    " LIMIT 20", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String tValue = qs.get("?mt").toString();
                String pValue = qs.get("?total").toString();
                logger.info("Title tValue:" + tValue + " pValue:" + pValue);
            }

            qe = QueryExecutionFactory.create(
                    "PREFIX bf:<http://id.loc.gov/ontologies/bibframe/> " +
                            "SELECT ?work ?workMainTitle ?exp " +
                            "WHERE {" +
                            "  ?work bf:title ?workTitle . ?workTitle bf:mainTitle ?workMainTitle ." +
                            "  ?work bf:expressionOf ?exp. " +
                            "  ?exp bf:title ?expTitle. " +
                            "  ?expTitle bf:mainTitle \"Strange case of Doctor Jekyll and Mister Hyde. Italian\"" +
                            "} " +
                            " LIMIT 20", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String wValue = qs.get("?work").toString();
                String wmtValue = qs.get("?workMainTitle").toString();
                String expValue = qs.get("?exp").toString();
                logger.info("Strange case of Doctor Jekyll and Mister Hyde. Italian:" + wValue + " workMainTitle:" + wmtValue + " expression:" + expValue);
            }

            // Releasing dataset resources
            dataset.close();
        } catch (Throwable t) {
            logger.error(WTF_MARKER, t.getMessage(), t);
        }
    }
}

