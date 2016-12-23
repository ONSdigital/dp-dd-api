package main;

import org.scalatest.testng.TestNGSuite;
import org.testng.annotations.Test;
import play.Logger;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;

import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static play.test.Helpers.fakeApplication;
import static play.test.Helpers.running;


public class LoadCsvToDatabaseTest extends TestNGSuite {

    EntityManagerFactory emf = Persistence.createEntityManagerFactory("data_discovery");
    EntityManager em = emf.createEntityManager();
    static Logger.ALogger logger = Logger.of(LoadCsvToDatabaseTest.class);

    static String datasetId = UUID.randomUUID().toString();

    PostgresTest postgresTest = new PostgresTest();

    @Test
    public void loadACsvIntoDb() throws Exception {
        running(fakeApplication(), () -> {
            EntityTransaction tx = em.getTransaction();
            tx.begin();
            try {

                postgresTest.createDatabase(em);
                postgresTest.createDataset(em, datasetId, "sectionA1.csv", "Title");
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
