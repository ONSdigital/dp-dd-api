package main;

import configuration.Configuration;
import org.scalatest.testng.TestNGSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import play.Logger;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;

import java.util.Map;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static play.test.Helpers.fakeApplication;
import static play.test.Helpers.running;


public class LoadCsvToDatabaseTest extends TestNGSuite {

    private EntityManagerFactory emf;
    private EntityManager em;

    static Logger.ALogger logger = Logger.of(LoadCsvToDatabaseTest.class);

    private static String datasetId = UUID.randomUUID().toString();

    private PostgresTest postgresTest;

    @BeforeClass
    public void setupJPA() {

        logger.info("SETTING UP JPA");
        final Map<String, Object> databaseParameters = Configuration.getDatabaseParameters();
        emf = Persistence.createEntityManagerFactory("data_discovery", databaseParameters);
        em = emf.createEntityManager();
    }

    @BeforeClass
    public void setupDb() {

        logger.info("SETTING UP DB");
        postgresTest = new PostgresTest();
    }

    @Test
    public void loadACsvIntoDb() throws Exception {

        logger.info("RUNNING loadACsvIntoDb");
        running(fakeApplication(), () -> {
            EntityTransaction tx = em.getTransaction();
            tx.begin();
            try {

                postgresTest.createDatabase(em);
                postgresTest.createDataset(em, datasetId, "Open-data-new-format.csv", "Title");
                assertEquals((long) em.createQuery("SELECT COUNT(ddp) from DimensionalDataPoint ddp where ddp.dimensionalDataSet.dimensionalDataSetId = :datasetId")
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