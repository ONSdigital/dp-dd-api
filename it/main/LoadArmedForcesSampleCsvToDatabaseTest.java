package main;

import configuration.Configuration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeGroups;
import org.testng.annotations.Test;
import play.Logger;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;
import java.util.Map;
import java.util.UUID;

import static junit.framework.TestCase.fail;
import static org.testng.Assert.assertEquals;
import static play.test.Helpers.fakeApplication;
import static play.test.Helpers.running;

public class LoadArmedForcesSampleCsvToDatabaseTest {

    private EntityManagerFactory emf;
    private EntityManager em;

    static Logger.ALogger logger = Logger.of(LoadCsvToDatabaseTest.class);

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

    @Test(enabled = false)
    public void loadArmedForcesSampleCsvIntoDb() throws Exception {

        logger.info("RUNNING loadArmedForcesDatasetCsvIntoDb");
        running(fakeApplication(), () -> {
            EntityTransaction tx = em.getTransaction();
            tx.begin();
            try {
                String datasetId = UUID.randomUUID().toString();
                postgresTest.createDatabase(em);
                postgresTest.createDataset(em, datasetId, "AF001EW-sample.csv", "Title - armed forces");
                assertEquals((long) em.createQuery("SELECT COUNT(ddp) from DimensionalDataPoint ddp where ddp.dimensionalDataSet.dimensionalDataSetId = :datasetId")
                        .setParameter("datasetId", UUID.fromString(datasetId)).getSingleResult(), 500L);

            } catch (Exception e) {
                e.printStackTrace();
                fail();
            } finally {
                tx.rollback();
            }
        });
    }
}
