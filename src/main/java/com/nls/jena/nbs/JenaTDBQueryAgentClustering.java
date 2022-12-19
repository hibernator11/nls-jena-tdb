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

public class JenaTDBQueryAgentClustering {
    private static Logger logger = LoggerFactory.getLogger(JenaTDBQueryAgentClustering.class);
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
            QueryExecution qe = qe = QueryExecutionFactory.create(
            "PREFIX bf:<http://id.loc.gov/ontologies/bibframe/> " +
                    "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#> " +
                    "SELECT ?label ?a " +
                    "WHERE {?s bf:contribution ?c . " +
                    " ?c bf:agent ?a ." +
                    " ?a rdfs:label ?label . " +
                    "FILTER regex(str(?a), \"http://\") " +
                    "FILTER regex(str(?label), \"Cree, James E.\") " +
                    "} LIMIT 10", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();

                String lValue = qs.get("?label").toString();
                String strValue = qs.get("?a").toString();
                logger.info("Cree, James E. " + lValue + " = " + strValue);
            }

            qe = QueryExecutionFactory.create(
                    "PREFIX bf:<http://id.loc.gov/ontologies/bibframe/> " +
                    "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#> " +
                    "SELECT ?label ?a " +
                    "WHERE {" +
                    " ?s bf:contribution ?c . " +
                    " ?c bf:agent ?a ." +
                    " ?a rdfs:label ?label . " +
                    "FILTER regex(str(?a), \"http://\") " +
                    "FILTER regex(str(?label), \"Stevenson, Robert Louis\") " +
                    "} LIMIT 10", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();

                String lValue = qs.get("?label").toString();
                String strValue = qs.get("?a").toString();
                logger.info("Stevenson, Robert Louis-> " + lValue + " = " + strValue);
            }

            qe = QueryExecutionFactory.create("SELECT ?p ?o " +
                    "WHERE {<http://example.org/9929751083804341#Hub240-10-Agent> ?p ?o . " +
                    "} limit 20", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();

                String pValue = qs.get("?p").toString();
                String strValue = qs.get("?o").toString();
                logger.info("http://example.org/9929751083804341#Hub240-10-Agent-> " + pValue + " = " + strValue);
            }

            qe = QueryExecutionFactory.create("SELECT * " +
                    "WHERE {<http://example.org/9929751083804341#Work> ?p ?o" +
                    "} limit 20", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();

                String pValue = qs.get("?p").toString();
                String strValue = qs.get("?o").toString();
                logger.info("http://example.org/9929751083804341#Work " + pValue + " = " + strValue);
            }

            // Releasing dataset resources
            dataset.close();
        } catch (Throwable t) {
            logger.error(WTF_MARKER, t.getMessage(), t);
        }
    }
}

