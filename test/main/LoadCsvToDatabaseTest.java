package main;

import org.scalatest.testng.TestNGSuite;
import org.testng.annotations.BeforeGroups;
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

    private PostgresTest postgresTest;

    @BeforeGroups("int-test")
    public void setupDb() {
        postgresTest = new PostgresTest();
    }

    @Test(groups="int-test")
    public void loadACsvIntoDb() throws Exception {
        logger.info("RUNNING loadACsvIntoDb");

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

    @Test(groups="unit-test")
    public void unitTest() throws Exception {
        logger.info("THIS IS A UNIT TEST");
    }

}
