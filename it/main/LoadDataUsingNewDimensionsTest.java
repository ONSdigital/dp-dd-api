package main;

import org.scalatest.testng.TestNGSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import play.Logger;
import services.InputCSVParser;
import uk.co.onsdigital.discovery.model.Dimension;
import uk.co.onsdigital.discovery.model.DimensionalDataSet;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static main.PostgresTest.*;
import static org.testng.Assert.assertEquals;

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
    public void loadDatasetAgainstNewDimensionWithoutHierarchies() throws Exception {

        String[] rowDataArray = "676767,,Geographic_Area,K04000001,,NACE,CI_0008197".split(",");

        EntityTransaction tx = em.getTransaction();
        tx.begin();
        try {
            logger.debug("\n\n####  Real test starts here  #####\n");

            new InputCSVParser().parseRowdataDirectToTablesFromTriplets(em, rowDataArray, dimensionalDataSet);

            List<Dimension> results = em.createQuery("SELECT d FROM Dimension d where d.dimensionalDataSetId = :dsid", Dimension.class).setParameter("dsid", datasetId).getResultList();

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
    public void loadDatasetAgainstNewDimensionWithHierarchies() throws Exception {

        String[] rowDataArray = "676767,2011STATH,Geographic_Area,K04000001,CL_0001480,NACE,CI_0008197".split(",");

        EntityTransaction tx = em.getTransaction();
        tx.begin();
        try {
            postgresTest.loadStandingData(em, Arrays.asList(_2011STATH_small));
            postgresTest.loadStandingData(em, Arrays.asList(COICOP));
            postgresTest.loadStandingData(em, Arrays.asList(NACE));
            assertEquals(em.createNativeQuery("select h from hierarchy h").getResultList().size(), 3);

            logger.debug("\n\n####  Real test starts here  #####\n");

            DimensionalDataSet dimensionalDataSet = postgresTest.createEmptyDataset(em, datasetId.toString(), "dataset");

            new InputCSVParser().parseRowdataDirectToTablesFromTriplets(em, rowDataArray, dimensionalDataSet);

            List<Dimension> results = em.createQuery("SELECT d FROM Dimension d where d.dimensionalDataSetId = :dsid", Dimension.class).setParameter("dsid", datasetId).getResultList();

            assertEquals(results.size(), 2);
            results.stream().forEach(r -> assertEquals(datasetId, r.getDimensionalDataSetId()));

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            tx.rollback();
        }
    }

}
