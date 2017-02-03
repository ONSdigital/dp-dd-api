package main;

import au.com.bytecode.opencsv.CSVParser;
import configuration.Configuration;
import exceptions.DatapointMappingException;
import exceptions.GLLoadException;
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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
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
    static String TIME = "../classification/time.sql";

    static String _2011GPH_SMALL = "../geo/2011GPH_small.sql";
    static String _2011GPH = "../geo/2011GPH.sql";
    static String _2013ADMIN = "../geo/2013ADMIN.sql";
    static String _2011STATH = "../geo/2011STATH.sql";
    static String _2011STATH_small = "../geo/2011STATH_small.sql";
    static String _2013WARDH = "../geo/2013WARDH.sql";

    static String COICOP = "../classification/CL_0000641_COICOP_Special_Aggregate.sql";
    static String NACE = "../classification/CL_0001480_NACE.sql";
    static String PRODCOM_ELEMENTS = "../classification/CL_0000737_Prodcom_Elements.sql";


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
        BufferedReader csvReader = getCSVBufferedReader(new File(new PostgresTest().getClass().getResource(inputFileName).getPath()));
        CSVParser csvParser = new CSVParser();

        if (csvReader != null) {
            try {
                csvReader.readLine();
                while (csvReader.ready() && (rowData = csvParser.parseLine(csvReader.readLine())) != null) {
                    parser.parseRowdataDirectToTables(em, rowData, dimensionalDataSet);
                }
            } finally {
                closeCSVReader(csvReader);
            }
        }
    }

    public BufferedReader getCSVBufferedReader(File inFile) {
        BufferedReader csvReader = null;
        try {
            csvReader = new BufferedReader(new InputStreamReader(new FileInputStream(inFile), "UTF-8"), 32768);
        } catch (IOException e) {
            logger.error("Failed to get the BufferedReader: ", e);
            throw new GLLoadException("Failed to get the BufferedReader: ", e);
        }
        return csvReader;
    }
    public void closeCSVReader(BufferedReader reader) {
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                logger.error("Failed while closing the CSVReader: ", e);
            }
        }
    }

}
