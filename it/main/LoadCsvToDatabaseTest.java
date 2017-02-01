package main;

import org.scalatest.testng.TestNGSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import play.Logger;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;

import java.util.Arrays;
import java.util.UUID;

import static main.PostgresTest.AREA_TYPES;
import static main.PostgresTest._2011GPH_SMALL;
import static org.testng.Assert.assertEquals;
import static play.test.Helpers.fakeApplication;
import static play.test.Helpers.running;


public class LoadCsvToDatabaseTest extends TestNGSuite {

    static Logger.ALogger logger = Logger.of(LoadCsvToDatabaseTest.class);

    private EntityManagerFactory emf;
    private EntityManager em;
    private PostgresTest postgresTest = new PostgresTest();

    @BeforeClass
    public void setupJPA() throws Exception {
        logger.info("SETTING UP JPA");
        emf = postgresTest.getEMFForEmptyTestDatabase();
        em = emf.createEntityManager();
    }

    @Test
    public void loadACsvIntoDb() throws Exception {

        logger.info("RUNNING loadACsvIntoDb");

        String datasetId = UUID.randomUUID().toString();

        running(fakeApplication(), () -> {
            EntityTransaction tx = em.getTransaction();
            tx.begin();
            try {
                postgresTest.loadStandingData(em, Arrays.asList(AREA_TYPES, _2011GPH_SMALL));

                postgresTest.createDatasetFromFile(em, datasetId, "Open-data-new-format.csv", "Title");
                assertEquals((long) em.createQuery("SELECT COUNT(ddp) from DimensionalDataPoint ddp where ddp.dimensionalDataSet.id = :datasetId")
                        .setParameter("datasetId", UUID.fromString(datasetId)).getSingleResult(), 276L);

            } catch (Exception e) {
                e.printStackTrace();
                fail();
            } finally {
                tx.rollback();
            }
        });
        }

    }