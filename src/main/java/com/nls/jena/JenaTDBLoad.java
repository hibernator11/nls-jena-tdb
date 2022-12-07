package com.nls.jena;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.tdb2.TDB2Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

public class JenaTDBLoad {
    private static Logger logger = LoggerFactory.getLogger(JenaTDBLoad.class);
    // Why This Failure marker
    private static final Marker WTF_MARKER = MarkerFactory.getMarker("WTF");

    public static void main(String[] args) {
        try {
            // Create dataset
            Path path = Paths.get(".").toAbsolutePath().normalize();
            String dbDir = path.toFile().getAbsolutePath() + "/db/";
            Location location = Location.create(dbDir);
            Dataset dataset = TDB2Factory.connectDataset(location);

            dataset.begin(ReadWrite.WRITE);
            Model model = dataset.getDefaultModel();

            //Files.walk(Paths.get("/home/gustavo/nls-fellowship/nls-fellowship-2022-23/rdf"))
            Files.walk(Paths.get(path.toFile().getAbsolutePath() +"/rdf"))
                    .filter(p -> p.toString().endsWith(".gz"))
                    .forEach(p -> {
                        logger.info(p.toFile().getAbsolutePath());
                        try {
                            RDFDataMgr.read(model, p.toFile().getAbsolutePath(), Lang.RDFXML);
                        }catch (Exception e){
                            logger.error(p.toFile().getAbsolutePath() + e.getMessage());
                        }
                    });
            dataset.commit();

            // Create transaction for writing
            //dataset.begin(ReadWrite.WRITE);
            //UpdateRequest updateRequest = UpdateFactory.create("INSERT DATA {<http://dbpedia.org/resource/Grace_Hopper> <http://xmlns.com/foaf/0.1/name> \"Grace Hopper\" .}");
            //UpdateProcessor updateProcessor = UpdateExecutionFactory.create(updateRequest, dataset);
            //updateProcessor.execute();
            //dataset.commit();

            // create transaction for reading
            dataset.begin(ReadWrite.READ);
            QueryExecution qe = QueryExecutionFactory.create("SELECT distinct ?type WHERE {?s a ?type .}", dataset);
            for (ResultSet results = qe.execSelect(); results.hasNext();) {
                QuerySolution qs = results.next();
                String strValue = qs.get("?type").toString();
                logger.trace("value = " + strValue);
            }

            // Releasing dataset resources
            dataset.close();
        } catch (Throwable t) {
            logger.error(WTF_MARKER, t.getMessage(), t);
        }
    }
}

