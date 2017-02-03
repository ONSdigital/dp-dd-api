package main;

import org.scalatest.testng.TestNGSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import play.Logger;
import uk.co.onsdigital.discovery.model.Hierarchy;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import java.util.Arrays;

import static main.PostgresTest.*;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;


public class LoadHierarchiesTest extends TestNGSuite {

    static Logger.ALogger logger = Logger.of(LoadHierarchiesTest.class);

    private EntityManagerFactory emf;
    private EntityManager em;
    private PostgresTest postgresTest = new PostgresTest();

    @BeforeClass
    public void setupDb() {
        emf = postgresTest.getEMFForEmptyTestDatabase();
        em = emf.createEntityManager();
    }

    @Test
    public void loadMultipleHierarchiesIntoSameDatabase() throws Exception {

        EntityTransaction tx = em.getTransaction();
        tx.begin();
        try {
            postgresTest.loadStandingData(em, Arrays.asList(COICOP));
            Hierarchy result = em.createQuery("SELECT h FROM Hierarchy h WHERE h.id = 'COICOP_TEST'", Hierarchy.class).getSingleResult();
            assertNotNull(result);

            postgresTest.loadStandingData(em, Arrays.asList(COICOP2));
            result = em.createQuery("SELECT h FROM Hierarchy h WHERE h.id = 'COICOP_TEST2'", Hierarchy.class).getSingleResult();
            assertNotNull(result);

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            tx.rollback();
        }
    }

    @Test
    public void loadABigHierarchyIntoDatabase() throws Exception {

        EntityTransaction tx = em.getTransaction();
        tx.begin();
        try {
            postgresTest.loadStandingData(em, Arrays.asList(_2011STATH));
            assertEquals(1, em.createNativeQuery("select h from hierarchy h where h.id='2011STATH'").getResultList().size());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            tx.rollback();
        }
    }

}
