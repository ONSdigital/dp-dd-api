package main;

import au.com.bytecode.opencsv.CSVParser;
import com.google.common.util.concurrent.UncheckedExecutionException;
import configuration.Configuration;
import configuration.DbMigrator;
import exceptions.DatapointMappingException;
import org.eclipse.persistence.platform.database.H2Platform;
import org.flywaydb.core.api.MigrationVersion;
import play.Logger;
import services.InputCSVParser;
import services.InputCSVParserV3;
import uk.co.onsdigital.discovery.model.DataResource;
import uk.co.onsdigital.discovery.model.DimensionalDataSet;

import javax.persistence.*;
import java.io.*;
import java.nio.file.Files;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.eclipse.persistence.config.PersistenceUnitProperties.*;


public class PostgresTest {

    static Logger.ALogger logger = Logger.of(PostgresTest.class);

    public static final MigrationVersion EMPTY_DB_VERSION = MigrationVersion.fromVersion("01.001");

    static String AREA_TYPES = "../geo/area_types.sql";
    static String TIME = "../classification/time.sql";

    static String _2011GPH_SMALL = "../geo/2011GPH_small.sql";
    static String _2011GPH = "../geo/2011GPH.sql";
    static String _2013ADMIN = "../geo/2013ADMIN.sql";
    static String _2011STATH_small = "../geo/2011STATH_small.sql";
    static String _2013WARDH = "../geo/2013WARDH.sql";

    static String COICOP = "/classification/COICOP_test.sql";
    static String COICOP2 = "/classification/COICOP_test2.sql";
    static String NACE = "../classification/CL_0001480_NACE.sql";
    static String PRODCOM_ELEMENTS = "../classification/CL_0000737_Prodcom_Elements.sql";



    public EntityManagerFactory getEMFForProductionLikeDatabase() {
        DbMigrator migrator = DbMigrator.getMigrator();
        migrator.getFlyway().setTarget(MigrationVersion.LATEST);
        migrator.migrate();
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("data_discovery", Configuration.getDatabaseParameters());
        return emf;
    }

    public EntityManagerFactory getEMFForEmptyTestDatabase() {
        Map<String, String> databaseParameters = new HashMap<String, String>() {{
            put(JDBC_URL, "jdbc:h2:mem:test");
            put(JDBC_USER, "SA");
            put(JDBC_PASSWORD, "");
            put(JDBC_DRIVER, "org.h2.Driver");
            put(DDL_GENERATION, DROP_AND_CREATE);
            put(DDL_GENERATION_MODE, DDL_DATABASE_GENERATION);
            put(TARGET_DATABASE, H2Platform.class.getName());
        }};
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("data_discovery", databaseParameters);
        return emf;
    }

    public void loadStandingData(EntityManager em, List<String> fileList) throws Exception {
        for (String file : fileList) {
            loadSomeData(em, file);
        }
    }

    private void loadSomeData(EntityManager em, String filename) throws Exception {
        logger.info("Loading data file {}", filename);
        File inputFile = new File(getClass().getResource(filename).getPath());

        // Raw JDBC is significantly faster than EclipseLink for bulk loading data, so unwrap the connection
        final Connection connection = em.unwrap(Connection.class);
        final Statement statement = connection.createStatement();
        final AtomicLong rows = new AtomicLong();
        final long batchSize = 5000L;

        Files.lines(inputFile.toPath()).forEach(line -> {
            try {
                statement.addBatch(line);
                if (rows.incrementAndGet() % batchSize == 0) {
                    logger.info("Processed {} rows of {}", rows.get(), filename);
                    statement.executeBatch();
                }

            } catch (SQLException e) {
                throw new UncheckedExecutionException(e);
            }
        });

        statement.executeBatch();
        logger.info("Finished loading {} rows of {}", rows.get(), filename);
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
        InputStream inputFileAsStream = getClass().getResourceAsStream(inputFileName);
        if(inputFileAsStream == null) {
            throw new RuntimeException("Input file not found!");
        }
        try (BufferedReader csvReader = new BufferedReader(new InputStreamReader(inputFileAsStream, "UTF-8"), 32768)) {
            CSVParser csvParser = new CSVParser();
            csvReader.readLine();
            while (csvReader.ready() && (rowData = csvParser.parseLine(csvReader.readLine())) != null) {
                parser.parseRowdataDirectToTables(em, rowData, dimensionalDataSet);
            }
        }
    }


}
