package main;

import org.scalatest.testng.TestNGSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import play.Logger;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;

public class LoadMultipleGeographicalHierarchiesTest extends TestNGSuite {

    static Logger.ALogger logger = Logger.of(LoadSingleDataPointToDatabaseTest.class);

    private EntityManagerFactory emf;
    private EntityManager em;
    private PostgresTest postgresTest = new PostgresTest();

    @BeforeClass
    public void setupDb() {
        logger.info("SETTING UP DB");
        emf = postgresTest.getEMFForEmptyTestDatabase();
        em = emf.createEntityManager();
    }

    @Test
    public void addSingleDataPointDirectly() throws Exception {

        logger.info("RUNNING addSingleDataPointDirectly");

        EntityTransaction tx = em.getTransaction();
        tx.begin();

        try {

            // TODO do something


        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            tx.commit();
        }
    }

}
