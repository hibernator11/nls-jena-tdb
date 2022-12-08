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

public class JenaTDBQueryRelations {
    private static Logger logger = LoggerFactory.getLogger(JenaTDBQueryRelations.class);
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

            // all the resources used in this property are Hub
            QueryExecution qe = QueryExecutionFactory.create(
                    "PREFIX bf:<http://id.loc.gov/ontologies/bibframe/> " +
                    "SELECT * " +
                    "WHERE {?s bf:expressionOf ?exp" +
                    //"        filter (!regex(str(?exp), \"Hub\"))" +
                            "} limit 50", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String strValue = qs.get("?exp").toString();
                logger.info("value = " + strValue);
            }

            qe = QueryExecutionFactory.create(
                    "PREFIX bf:<http://id.loc.gov/ontologies/bibframe/> " +
                            "SELECT * " +
                            "WHERE {?s bf:expressionOf <http://example.org/998311393804341#Hub240-11> .?s ?p ?o ." +
                            //"        filter (!regex(str(?exp), \"Hub\"))" +
                            "} limit 30", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String sValue = qs.get("?s").toString();
                String pValue = qs.get("?p").toString();
                String oValue = qs.get("?o").toString();
                logger.info("value = " + sValue + " " + pValue + " " + oValue);
            }

            // let's check what a Hub contains
            qe = QueryExecutionFactory.create(
                    "PREFIX bf:<http://id.loc.gov/ontologies/bibframe/> " +
                            "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#> " +
                            "SELECT * " +
                            "WHERE {<http://example.org/998311393804341#Hub240-11> ?p ?c." +
                            "} limit 10", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();

                String oValue = qs.get("?p").toString();
                logger.info("http://example.org/998311393804341#Hub240-11 p value = " + oValue);
            }

            // let's check what a Hub contains
            qe = QueryExecutionFactory.create(
                    "PREFIX bf:<http://id.loc.gov/ontologies/bibframe/> " +
                            "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#> " +
                            "SELECT * " +
                            "WHERE {<http://example.org/998311393804341#Hub240-11> bf:title ?c." +
                            " ?c bf:mainTitle ?label ." +
                            "} limit 10", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();

                String oValue = qs.get("?label").toString();
                logger.info("http://example.org/998311393804341#Hub240-11 bf:title value = " + oValue);
            }

            // let's check what a Hub contains
            qe = QueryExecutionFactory.create(
                    "PREFIX bf:<http://id.loc.gov/ontologies/bibframe/> " +
                            "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#> " +
                            "SELECT * " +
                            "WHERE {<http://example.org/998311393804341#Hub240-11> bf:contribution ?c." +
                            " ?c bf:agent ?agent . ?agent rdfs:label ?label ." +
                            "} limit 10", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();

                String oValue = qs.get("?label").toString();
                logger.info("http://example.org/998311393804341#Hub240-11 bf:contribution value = " + oValue);
            }


            qe = QueryExecutionFactory.create(
                    "PREFIX bf:<http://id.loc.gov/ontologies/bibframe/> " +
                            "SELECT * " +
                            "WHERE {<http://example.org/99115986384804341#Hub130-7> bf:title ?t . ?t ?p ?o" +
                            "} limit 10", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String strValue = qs.get("?p").toString();
                String oValue = qs.get("?o").toString();
                logger.info("<http://example.org/99115986384804341#Hub130-7> = " + strValue + " " + oValue);
            }



            // Releasing dataset resources
            dataset.close();
        } catch (Throwable t) {
            logger.error(WTF_MARKER, t.getMessage(), t);
        }
    }
}


