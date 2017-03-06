package main;

import org.scalatest.testng.TestNGSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import play.Logger;
import uk.co.onsdigital.discovery.model.DataSet;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import java.util.UUID;

public class LoadV3CPITest extends TestNGSuite {

    static Logger.ALogger logger = Logger.of(LoadV3CPITest.class);

    private EntityManagerFactory emf;
    private EntityManager em;
    private PostgresTest postgresTest = new PostgresTest();

    UUID datasetId;
    DataSet dataSet;

    @BeforeClass
    public void setupDb() {
        emf = postgresTest.getEMFForProductionLikeDatabase();
        em = emf.createEntityManager();
    }

    @BeforeMethod
    public void setup() {
        datasetId = UUID.randomUUID();
        dataSet = postgresTest.createEmptyDataset(em, datasetId.toString(), "dataset");
    }

    @Test
    public void loadCPI_2016_12_COICOIP_v3() throws Exception {
        postgresTest.loadFileAndCheckDimensionCount(em, datasetId, "CPI_2016_12_COICOP_v3.csv", 138);
    }

    @Test
    public void loadCPI_2016_12_SpecAgg_v3() throws Exception {
        postgresTest.loadFileAndCheckDimensionCount(em, datasetId, "CPI_2016_12_SpecialAggregate_v3.csv", 55);
    }

    @Test
    public void loadCPI_1996_Jan2017_COICOP_v3() throws Exception {
        postgresTest.loadFileAndCheckDimensionCount(em, datasetId, "CPI_1996-Jan2017_COICOP_v3.csv", 390);
    }

    @Test
    public void loadCPI_1996_Jan2017_SpecialAggregate_v3() throws Exception {
        postgresTest.loadFileAndCheckDimensionCount(em, datasetId, "CPI_1996-Jan2017_SpecialAggregate_v3.csv", 307);
    }

}
