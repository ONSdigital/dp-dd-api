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

public class LoadV3AnnualBusinessSurveyTest extends TestNGSuite {

    static Logger.ALogger logger = Logger.of(LoadV3AnnualBusinessSurveyTest.class);

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
    public void loadAnnualBusinessSurvey_Employment_v3() throws Exception {
        postgresTest.loadFileAndCheckDimensionCount(em, datasetId, "AnnualBusinessSurvey_Employment_v3.csv", 949);
    }

    @Test
    public void loadAnnualBusinessSurvey_NumberOfEnterprises_v3() throws Exception {
        postgresTest.loadFileAndCheckDimensionCount(em, datasetId, "AnnualBusinessSurvey_NumberOfEnterprises_v3.csv", 944);
    }

    @Test
    public void loadAnnualBusinessSurvey_BusinessValue_v3() throws Exception {
        postgresTest.loadFileAndCheckDimensionCount(em, datasetId, "AnnualBusinessSurvey_UKBusinessValue_v3_small.csv", 39);
//        postgresTest.loadFileAndCheckDimensionCount(em, datasetId, "AnnualBusinessSurvey_UKBusinessValue_v3.csv", 983); // careful - this takes a few minutes
    }

}
