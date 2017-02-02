package main;

import au.com.bytecode.opencsv.CSVParser;
import org.scalatest.testng.TestNGSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import play.Logger;
import services.InputCSVParserV3;
import uk.co.onsdigital.discovery.model.DimensionValue;
import uk.co.onsdigital.discovery.model.DimensionalDataSet;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import java.io.BufferedReader;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static main.PostgresTest.*;
import static org.testng.Assert.assertEquals;
import static play.test.Helpers.fakeApplication;
import static play.test.Helpers.running;

public class LoadV3InputFilesTest extends TestNGSuite {

    static Logger.ALogger logger = Logger.of(LoadV3InputFilesTest.class);

    private EntityManagerFactory emf;
    private EntityManager em;
    private PostgresTest postgresTest = new PostgresTest();

    UUID datasetId;
    DimensionalDataSet dimensionalDataSet;

    @BeforeClass
    public void setupDb() {
        emf = postgresTest.getEMFForEmptyTestDatabase();
        em = emf.createEntityManager();
    }

    @BeforeMethod
    public void setup() {
        datasetId = UUID.randomUUID();
        dimensionalDataSet = postgresTest.createEmptyDataset(em, datasetId.toString(), "dataset");
    }


    @Test
    public void loadAF001EW_v3_ARMED_FORCES() throws Exception {

        running(fakeApplication(), () -> {

            EntityTransaction tx = em.getTransaction();
            tx.begin();
            try {

                String inputFileName = "AF001EW_v3.csv";
                postgresTest.loadStandingData(em, Arrays.asList(TIME, _2011STATH));

                // TODO - this all belongs in the v3CsvParser - replace with single method call
                String rowData[];
                InputCSVParserV3 parser = new InputCSVParserV3();
                BufferedReader csvReader = parser.getCSVBufferedReader(new File(new PostgresTest().getClass().getResource(inputFileName).getPath()));
                CSVParser csvParser = new CSVParser();
                DimensionalDataSet dimensionalDataSet = postgresTest.createEmptyDataset(em, datasetId.toString(), "dataset");

                if (csvReader != null) {
                    try {
                        csvReader.readLine();
                        while (csvReader.ready() && (rowData = csvParser.parseLine(csvReader.readLine())) != null) {

                            parser.parseRowdataDirectToTablesFromTriplets(em, rowData, dimensionalDataSet);
                        }
                    } finally {
                        parser.closeCSVReader(csvReader);
                    }
                }

                List<DimensionValue> dimensionValues= em.createQuery("SELECT dim from DimensionValue dim where dim.dimensionalDataSetId = :datasetId")
                        .setParameter("datasetId", datasetId)
                        .getResultList();

                assertEquals(dimensionValues.size(), 247);  // i am accepting this number as correct having manually checked a subset of the file

            } catch (Exception e) {
                e.printStackTrace();
                fail();
            } finally {
                tx.rollback();
            }
        });
    }



}
