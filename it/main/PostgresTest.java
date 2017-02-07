package main;

import au.com.bytecode.opencsv.CSVParser;
import configuration.Configuration;
import org.eclipse.persistence.config.PersistenceUnitProperties;
import play.Logger;
import services.InputCSVParser;
import services.InputCSVParserV3;
import uk.co.onsdigital.discovery.model.DataResource;
import uk.co.onsdigital.discovery.model.DimensionalDataSet;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;


public class PostgresTest {

    static Logger.ALogger logger = Logger.of(PostgresTest.class);

    static String AREA_TYPES = "../geo/area_types.sql";

    static String _2011GPH_SMALL = "../geo/2011GPH_small.sql";
    static String _2011GPH = "../geo/2011GPH.sql";
    static String _2013ADMIN = "../geo/2013ADMIN.sql";
    static String COICOP = "/classification/COICOP_test.sql";
    static String COICOP2 = "/classification/COICOP_test2.sql";



    public EntityManagerFactory getEMFForProductionLikeDatabase() {
        Map<String, Object> databaseParameters = Configuration.getDatabaseParameters();
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("data_discovery", databaseParameters);
        return emf;
    }

    public EntityManagerFactory getEMFForEmptyTestDatabase() {
        Map<String, Object> databaseParameters = Configuration.getDatabaseParameters();
        databaseParameters.put(PersistenceUnitProperties.ECLIPSELINK_PERSISTENCE_XML, "META-INF/persistence_test.xml");
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("data_discovery", databaseParameters);
        return emf;
    }

    public void loadStandingData(EntityManager em, List<String> fileList) throws Exception {
        for (String file : fileList) {
            loadSomeData(em, file);
        }
    }

    private void loadSomeData(EntityManager em, String filename) throws Exception {
        File inputFile = new File(new PostgresTest().getClass().getResource(filename).getPath());

        ArrayList<String> sqlScripts = Files.lines(Paths.get(inputFile.getAbsolutePath()))
                .filter(line -> !line.startsWith("--") && !line.isEmpty()).collect(Collectors.toCollection(ArrayList::new));

        sqlScripts.forEach(ss -> {
            Query q = em.createNativeQuery(ss);
            q.executeUpdate();
        });
    }


    public void createDatasetFromFile(EntityManager em, String id, String filename, String title) throws Exception {
        logger.debug("\n\n########   Start createDatasetFromFile ###########\n\n");

        File inputFile = new File(new PostgresTest().getClass().getResource(filename).getPath());
        DimensionalDataSet dimensionalDataSet = createEmptyDataset(em, id, title);

        long startTime = System.nanoTime();
        new InputCSVParser().run(em, dimensionalDataSet, inputFile);
        long endTime = System.nanoTime();

        long duration = (endTime - startTime) / 1000000; // seconds
        logger.debug("\n\n###### Process took " + duration + " millis ######");
        em.flush();
        em.clear();

    }

    public DimensionalDataSet createEmptyDataset(EntityManager em, String id, String title) {
        // todo this belongs as part of the csv 'import' function
        DimensionalDataSet dimensionalDataSet = em.find(DimensionalDataSet.class, UUID.fromString(id));
        if (dimensionalDataSet == null) {
            DataResource resource = new DataResource(id, "title");
            em.persist(resource);
            dimensionalDataSet = new DimensionalDataSet(title, resource);
            dimensionalDataSet.setId(UUID.fromString(id));
            em.persist(dimensionalDataSet);
        }
        return dimensionalDataSet;
    }

    public void loadEachLineInV3File(EntityManager em, String inputFileName, DimensionalDataSet dimensionalDataSet) throws IOException, DatapointMappingException {
        String rowData[];
        InputCSVParserV3 parser = new InputCSVParserV3();
        try (BufferedReader csvReader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(inputFileName), "UTF-8"), 32768)) {
            CSVParser csvParser = new CSVParser();
            csvReader.readLine();
            while (csvReader.ready() && (rowData = csvParser.parseLine(csvReader.readLine())) != null) {
                parser.parseRowdataDirectToTables(em, rowData, dimensionalDataSet);
            }
        }
    }


}
