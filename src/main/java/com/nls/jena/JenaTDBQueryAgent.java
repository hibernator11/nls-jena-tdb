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

public class JenaTDBQueryAgent {
    private static Logger logger = LoggerFactory.getLogger(JenaTDBQueryAgent.class);
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
                    "WHERE {?s a <http://id.loc.gov/ontologies/bibframe/Agent> " +
                    "filter regex(str(?s), \"#Agent\") " +
                    "}", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String strValue = qs.get("?total").toString();
                logger.info("number of Agents = " + strValue);
            }

            qe = QueryExecutionFactory.create("SELECT ?s " +
                    "WHERE {?s a <http://id.loc.gov/ontologies/bibframe/Agent>. " +
                    "filter regex(str(?s), \"#Agent\") " +
                    "} limit 20", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String sValue = qs.get("?s").toString();
                logger.info("URLs Agent:" + sValue );
            }

            qe = QueryExecutionFactory.create("SELECT ?s ?p ?o " +
                    "WHERE {?s a <http://id.loc.gov/ontologies/bibframe/Agent>. ?s ?p ?o} limit 10", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String sValue = qs.get("?s").toString();
                String pValue = qs.get("?p").toString();
                String oValue = qs.get("?o").toString();
                logger.info("sValue:" + sValue + " pValue:" + pValue + " oValue:" + oValue);
            }

            qe = QueryExecutionFactory.create("SELECT ?p ?o " +
                    "WHERE {<http://example.org/99116414544204341#Agent100-9> ?p ?o}", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String pValue = qs.get("?p").toString();
                String oValue = qs.get("?o").toString();
                logger.info("http://example.org/99116414544204341#Agent100-9 pValue:" + pValue + " oValue:" + oValue);
            }


            qe = QueryExecutionFactory.create("SELECT ?s ?p " +
                    "WHERE {?s ?p <http://example.org/99116414544204341#Agent100-9>}", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String sValue = qs.get("?s").toString();
                String pValue = qs.get("?p").toString();
                logger.info("sValue:" + sValue + " pValue:" + pValue + " http://example.org/99116414544204341#Agent100-9");
            }

            qe = QueryExecutionFactory.create("PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#> " +
                    "SELECT distinct ?l " +
                    "WHERE {?s a <http://id.loc.gov/ontologies/bibframe/Role> . ?s rdfs:label ?l}", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String lValue = qs.get("?l").toString();

                logger.info("Roles:" + lValue);
            }

            qe = QueryExecutionFactory.create("PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#> " +
                    "SELECT *" +
                    "WHERE {?s a <http://id.loc.gov/ontologies/bibframe/Isni> . ?s ?p ?o} limit 10", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String sValue = qs.get("?s").toString();
                String pValue = qs.get("?p").toString();
                String oValue = qs.get("?o").toString();

                logger.info("Isni:" + sValue + " " + pValue + " " + oValue);
            }

            // Releasing dataset resources
            dataset.close();
        } catch (Throwable t) {
            logger.error(WTF_MARKER, t.getMessage(), t);
        }
    }
}

