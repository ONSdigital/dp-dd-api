package main;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import play.Logger;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import java.util.UUID;

import static junit.framework.TestCase.fail;
import static org.testng.Assert.assertEquals;
import static play.test.Helpers.fakeApplication;
import static play.test.Helpers.running;

public class LoadArmedForcesSampleCsvToDatabaseTest {

    static Logger.ALogger logger = Logger.of(LoadCsvToDatabaseTest.class);

    private EntityManagerFactory emf;
    private EntityManager em;
    private PostgresTest postgresTest = new PostgresTest();

    @BeforeClass
    public void setupJPA() {
        logger.info("SETTING UP JPA");
        emf = postgresTest.getEMFForProductionLikeDatabase();
        em = emf.createEntityManager();
    }

    @Test
    public void loadArmedForcesSampleCsvIntoDb() throws Exception {

        logger.info("RUNNING loadArmedForcesDatasetCsvIntoDb");
        running(fakeApplication(), () -> {
            EntityTransaction tx = em.getTransaction();
            tx.begin();
            try {
                String datasetId = UUID.randomUUID().toString();
                postgresTest.createDatasetFromFile(em, datasetId, "AF001EW-sample.csv", "Title - armed forces");
                assertEquals((long) em.createQuery("SELECT COUNT(ddp) from DimensionalDataPoint ddp where ddp.dimensionalDataSet.id = :datasetId")
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
