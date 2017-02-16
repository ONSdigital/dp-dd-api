package main;

import org.scalatest.testng.TestNGSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import play.Logger;
import uk.co.onsdigital.discovery.model.DimensionValue;
import uk.co.onsdigital.discovery.model.DimensionalDataSet;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import java.util.List;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static play.test.Helpers.fakeApplication;
import static play.test.Helpers.running;

public class LoadV3AnnualBusinessSurveyTest extends TestNGSuite {

    static Logger.ALogger logger = Logger.of(LoadV3AnnualBusinessSurveyTest.class);

    private EntityManagerFactory emf;
    private EntityManager em;
    private PostgresTest postgresTest = new PostgresTest();

    UUID datasetId;
    DimensionalDataSet dimensionalDataSet;

    @BeforeClass
    public void setupDb() {
        emf = postgresTest.getEMFForProductionLikeDatabase();
        em = emf.createEntityManager();
    }

    @BeforeMethod
    public void setup() {
        datasetId = UUID.randomUUID();
        dimensionalDataSet = postgresTest.createEmptyDataset(em, datasetId.toString(), "dataset");
    }

    @Test
    public void loadAnnualBusinessSurvey_Employment_v3() throws Exception {
        loadFileAndCheckDimensionCount("AnnualBusinessSurvey_Employment_v3.csv", 949);
    }

    @Test
    public void loadAnnualBusinessSurvey_NumberOfEnterprises_v3() throws Exception {
        loadFileAndCheckDimensionCount("AnnualBusinessSurvey_NumberOfEnterprises_v3.csv", 944);
    }

    @Test
    public void loadAnnualBusinessSurvey_BusinessValue_v3() throws Exception {
        loadFileAndCheckDimensionCount("AnnualBusinessSurvey_UKBusinessValue_v3_small.csv", 39);
//        loadFileAndCheckDimensionCount("AnnualBusinessSurvey_UKBusinessValue_v3.csv", 983); // careful - this takes a few minutes
    }


    private void loadFileAndCheckDimensionCount(String inputFileName, int expectedNumberOfDimensions) {
        running(fakeApplication(), () -> {

            EntityTransaction tx = em.getTransaction();
            tx.begin();
            try {
                logger.debug("Loading file " + inputFileName + " ...");
                postgresTest.loadEachLineInV3File(em, inputFileName, postgresTest.createEmptyDataset(em, datasetId.toString(), "dataset"));

                List<DimensionValue> dimensionValues= em.createQuery("SELECT dim from DimensionValue dim where dim.dimension.dataSet.id = :datasetId", DimensionValue.class)
                        .setParameter("datasetId", datasetId)
                        .getResultList();

                assertEquals(dimensionValues.size(), expectedNumberOfDimensions);

            } catch (Exception e) {
                e.printStackTrace();
                fail();
            } finally {
                tx.rollback();
            }
        });
    }

}
