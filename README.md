# nls-jena-tdb
Java project for the analysis of the RDF data generated from the National Bibliography of Scotland.

## Note
Created in October-December 2022 for the National Library of Scotland's Data Foundry by [Gustavo Candela, National Librarian’s Research Fellowship in Digital Scholarship 2022-23](https://data.nls.uk/projects/the-national-librarians-research-fellowship-in-digital-scholarship-2022-23/)

In particular, this project uses the [RDF](https://www.w3.org/RDF/) dataset generated based on the National Bibliography of Scotland dataset published by the National Library of Scotland. See more details in [this link] (https://github.com/hibernator11/nls-fellowship-2022-23#national-bibliography-of-scotland).

## Structure of the project
This project is based on Java and Maven. It requires Maven installed in your computer to be able to run the project.

The following image describes the structure of the project.

- db: folder used by JenaTDB to load and store the RDF dataset
- dbtest: folder used by JenaTDB to load and store the RDF samples provided in the rdf folder
- logs: log of the code
- rdf: examples of RDF files created from the National Bibliography of Scotland using the tool [marc2bibframe2](https://github.com/lcnetdev/marc2bibframe2)
- src/main/java: main Java classes included in the project (described below)
- src/main/resources: additional files such as [log4j2.xml](https://logging.apache.org/log4j/2.x/manual/configuration.html) to configure the log (trace,warn,error,info).
- pom.xml: contains information to build the project such as dependencies, build directory, source directory,...In order to be able to work with Jena TDB, this file includes the dependency [apache-jena-libs](https://mvnrepository.com/artifact/org.apache.jena/apache-jena-libs)

<img src="images/structure-project.png">


Several classes are provided to analyse the content of the dataset. These are the classes found in the project:

- JenaTDBLoad: loading the RDF files provided in the folder rdf. This path can be changed in order to provide the folder with the RDF files.  
- JenaTDBLoadTest: loading the RDF files provided in the folder rdf for testing purposes.


- JenaTDBQuery: 
- JenaTDBQueryAgent:
- JenaTDBQueryCluestering:
- JenaTDBQueryGeographicCoverage:
- JenaTDBQueryInstance:
- JenaTDBQueryLanguage:
- JenaTDBQueryTitle:
- JenaTDBQueryWork:


## Running the project
Beffore running the project, we need to download the libraries and compile the code. We need to run the command:

```
mvn clean install
```

