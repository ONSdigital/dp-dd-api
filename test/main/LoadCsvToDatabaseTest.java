package main;

import org.scalatest.testng.TestNGSuite;
import org.testng.annotations.Test;
import play.Logger;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;

import static org.testng.Assert.assertEquals;


public class LoadCsvToDatabaseTest extends TestNGSuite {

    EntityManagerFactory emf = Persistence.createEntityManagerFactory("data_discovery");
    EntityManager em = emf.createEntityManager();
    static Logger.ALogger logger = Logger.of(LoadCsvToDatabaseTest.class);

    static String datasetId = "666";

    PostgresTest postgresTest = new PostgresTest();

    @Test
    public void loadACsvIntoDb() throws Exception {
        try {
            EntityTransaction tx = em.getTransaction();
            tx.begin();

            postgresTest.createDatabase(em);
            postgresTest.createDataset(em, datasetId, "Open-Data-small.csv", "Title");
            assertEquals(em.createQuery("SELECT ddp from DimensionalDataPoint ddp where ddp.dimensionalDataSet.dataResourceBean.dataResource = '666'").getResultList().size(), 276);

            tx.rollback();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

}
