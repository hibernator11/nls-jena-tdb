package com.nls.jena;

import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.tdb2.TDB2Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;

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
            /*for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String strValue = qs.get("?type").toString();
                //logger.info("value = " + strValue);
            }

            qe = QueryExecutionFactory.create("SELECT (COUNT(distinct ?type) AS ?total) " +
                    "WHERE {?s a ?type }", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String strValue = qs.get("?total").toString();
                //logger.info("number of classes = " + strValue);
            }

            qe = QueryExecutionFactory.create("SELECT (COUNT(distinct ?s) AS ?total) " +
                    "WHERE {?s a <http://id.loc.gov/ontologies/bibframe/Work> . " +
                    "?s <http://id.loc.gov/ontologies/bibframe/hasInstance> ?instance . " +
                    "filter regex(str(?s), \"http\") " +
                    "}", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String strValue = qs.get("?total").toString();
                logger.info("number of bf:Work = " + strValue);
            }

            qe = QueryExecutionFactory.create("SELECT (COUNT(distinct ?s) AS ?total) " +
                    "WHERE {?s a <http://id.loc.gov/ontologies/bibframe/Agent> . " +
                    "filter regex(str(?s), \"http://\") " +
                    "}", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String strValue = qs.get("?total").toString();
                logger.info("number of bf:Agent = " + strValue);
            }*/

            /*qe = QueryExecutionFactory.create("SELECT ?a ?label (COUNT(?s) AS ?total) " +
                    "WHERE {?s <http://id.loc.gov/ontologies/bibframe/contribution> ?c . " +
                    " ?c <http://id.loc.gov/ontologies/bibframe/agent> ?a ." +
                    " ?a <http://www.w3.org/2000/01/rdf-schema#label> ?label " +
                    "filter regex(str(?a), \"http://\") " +
                    "} group by ?a ?label " +
                    " limit 10", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String aValue = qs.get("?a").toString();
                String lValue = qs.get("?label").toString();
                String strValue = qs.get("?total").toString();
                logger.info("count agents number of " + aValue+ " " + lValue + " = " + strValue);
            }*/

            /*qe = QueryExecutionFactory.create("SELECT ?label (COUNT(?s) AS ?total) " +
                    "WHERE {?s <http://id.loc.gov/ontologies/bibframe/contribution> ?c . " +
                    " ?c <http://id.loc.gov/ontologies/bibframe/agent> ?a ." +
                    " ?a <http://www.w3.org/2000/01/rdf-schema#label> ?label " +
                    "filter regex(str(?a), \"http://\") " +
                    "} group by ?label " +
                    " limit 10", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();

                String lValue = qs.get("?label").toString();
                String strValue = qs.get("?total").toString();
                logger.info("count agents number of " + lValue + " = " + strValue);
            }*/




            /*qe = QueryExecutionFactory.create("SELECT distinct ?p " +
                    "WHERE {?s ?p ?o }", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String strValue = qs.get("?p").toString();
                //logger.info("value = " + strValue);
            }

            qe = QueryExecutionFactory.create("SELECT (COUNT(distinct ?p) AS ?total) " +
                    "WHERE {?s ?p ?o }", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String strValue = qs.get("?total").toString();
                logger.info("number of properties = " + strValue);
            }*/

            // Releasing dataset resources
            dataset.close();
        } catch (Throwable t) {
            logger.error(WTF_MARKER, t.getMessage(), t);
        }
    }
}


