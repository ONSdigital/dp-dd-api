package main;

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
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static main.PostgresTest.*;
import static org.testng.Assert.assertEquals;
import static play.test.Helpers.fakeApplication;
import static play.test.Helpers.running;

public class LoadDataUsingNewDimensionsTest extends TestNGSuite {

    static Logger.ALogger logger = Logger.of(LoadDataUsingNewDimensionsTest.class);

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
    public void loadSingleDatapointAgainstNewDimensionWithoutHierarchies() throws Exception {

        String[] rowDataArray = "676767,,,,Geographic_Area,K04000001,,NACE,CI_0008197".split(",");

        EntityTransaction tx = em.getTransaction();
        tx.begin();
        try {
            logger.debug("\n\n####  Real test starts here  #####\n");

            new InputCSVParserV3().parseRowdataDirectToTables(em, rowDataArray, dimensionalDataSet);

            List<DimensionValue> results = em.createQuery("SELECT d FROM DimensionValue d where d.dimensionalDataSetId = :dsid", DimensionValue.class).setParameter("dsid", datasetId).getResultList();

            assertEquals(results.size(), 2);
            results.stream().forEach(r -> assertEquals(datasetId, r.getDimensionalDataSetId()));

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            tx.rollback();
        }
    }

    @Test
    public void loadSingleDatapointAgainstNewDimensionWithHierarchies() throws Exception {

        String[] rowDataArray = "676767,,,2011STATH,Geographic_Area,K04000001,CL_0001480,NACE,CI_0008197".split(",");

        EntityTransaction tx = em.getTransaction();
        tx.begin();
        try {
            postgresTest.loadStandingData(em, Arrays.asList(_2011STATH_small));
            postgresTest.loadStandingData(em, Arrays.asList(COICOP));
            postgresTest.loadStandingData(em, Arrays.asList(NACE));
            assertEquals(em.createNativeQuery("SELECT h FROM hierarchy h").getResultList().size(), 3);

            logger.debug("\n\n####  Real test starts here  #####\n");

            DimensionalDataSet dimensionalDataSet = postgresTest.createEmptyDataset(em, datasetId.toString(), "dataset");

            new InputCSVParserV3().parseRowdataDirectToTables(em, rowDataArray, dimensionalDataSet);

            List<DimensionValue> results = em.createQuery("SELECT d FROM DimensionValue d where d.dimensionalDataSetId = :dsid", DimensionValue.class)
                    .setParameter("dsid", datasetId)
                    .getResultList();

            assertEquals(results.size(), 2);
            results.stream().forEach(r -> assertEquals(datasetId, r.getDimensionalDataSetId()));

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            tx.rollback();
        }
    }


    @Test
    public void loadAV3InputFileIntoDb() throws Exception {

        String datasetId = UUID.randomUUID().toString();

        running(fakeApplication(), () -> {

            EntityTransaction tx = em.getTransaction();
            tx.begin();
            try {
                postgresTest.loadStandingData(em, Arrays.asList(TIME, _2011STATH_small, NACE, PRODCOM_ELEMENTS));
                postgresTest.loadEachLineInV3File(em, "Open-Data-v3.csv", postgresTest.createEmptyDataset(em, datasetId.toString(), "dataset"));

                assertEquals((long) em.createQuery("SELECT COUNT(dim) from DimensionValue dim where dim.dimensionalDataSetId = :datasetId")
                        .setParameter("datasetId", UUID.fromString(datasetId))
                        .getSingleResult(), 51L);

                DimensionValue dimension = em.createQuery("SELECT dim from DimensionValue dim where dim.name = :dimName AND dim.value = :dimValue", DimensionValue.class)
                        .setParameter("dimName", "NACE")
                        .setParameter("dimValue", "CI_0008168")
                        .getSingleResult();

                 long dimensionValuesCount = (long) em.createQuery("SELECT COUNT(dp) FROM DataPoint dp where :dim1 MEMBER OF dp.dimensionValues")
                        .setParameter("dim1", dimension)
                        .getSingleResult();

                assertEquals(dimensionValuesCount, 9);

            } catch (Exception e) {
                e.printStackTrace();
                fail();
            } finally {
                tx.rollback();
            }
        });
    }

}
