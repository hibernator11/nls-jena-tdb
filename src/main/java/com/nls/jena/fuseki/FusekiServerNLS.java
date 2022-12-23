package com.nls.jena.fuseki;

import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.query.Dataset;
import org.apache.jena.tdb2.TDB2Factory;

import java.nio.file.Path;
import java.nio.file.Paths;

public class FusekiServerNLS {
    public static void main(String[] args) {
        // Create dataset
        Path path = Paths.get(".").toAbsolutePath().normalize();
        String dbDir = path.toFile().getAbsolutePath() + "/db/";
        Location location = Location.create(dbDir);
        Dataset dataset = TDB2Factory.connectDataset(location);

        FusekiServer server = FusekiServer.create()
                .add("/rdf", dataset)
                .build() ;
        server.start();

        //server.stop();
    }
}
