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
                postgresTest.loadStandingData(em, Arrays.asList(TIME, _2011STATH));
                postgresTest.loadEachLineInV3File(em, "AF001EW_v3.csv", postgresTest.createEmptyDataset(em, datasetId.toString(), "dataset"));

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

    @Test
    public void loadCPI_2016_12_COICOIP_v3() throws Exception {

        running(fakeApplication(), () -> {

            EntityTransaction tx = em.getTransaction();
            tx.begin();
            try {
                postgresTest.loadStandingData(em, Arrays.asList(TIME, COICOP));
                postgresTest.loadEachLineInV3File(em, "CPI_2016_12_COICOP_v3.csv", postgresTest.createEmptyDataset(em, datasetId.toString(), "dataset"));

                List<DimensionValue> dimensionValues= em.createQuery("SELECT dim from DimensionValue dim where dim.dimensionalDataSetId = :datasetId")
                        .setParameter("datasetId", datasetId)
                        .getResultList();

                assertEquals(dimensionValues.size(), 138);

            } catch (Exception e) {
                e.printStackTrace();
                fail();
            } finally {
                tx.rollback();
            }
        });
    }

    @Test
    public void loadCPI_2016_12_SpecAgg_v3() throws Exception {

        running(fakeApplication(), () -> {

            EntityTransaction tx = em.getTransaction();
            tx.begin();
            try {
                postgresTest.loadStandingData(em, Arrays.asList(TIME, COICOP));
                postgresTest.loadEachLineInV3File(em, "CPI_2016_12_SpecialAggregate_v3.csv", postgresTest.createEmptyDataset(em, datasetId.toString(), "dataset"));

                List<DimensionValue> dimensionValues= em.createQuery("SELECT dim from DimensionValue dim where dim.dimensionalDataSetId = :datasetId")
                        .setParameter("datasetId", datasetId)
                        .getResultList();

                assertEquals(dimensionValues.size(), 55);

            } catch (Exception e) {
                e.printStackTrace();
                fail();
            } finally {
                tx.rollback();
            }
        });
    }



}
